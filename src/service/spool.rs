use super::{Error, InputData};
use fs2::FileExt;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct SpoolManager {
    root_dir: PathBuf,
    pub(super) run_dir: PathBuf,
    run_lock: Option<File>,
    next_file_id: AtomicU64,
}

impl SpoolManager {
    pub(super) fn new() -> Result<Self, Error> {
        let root_dir = resolve_spool_root_dir();
        fs::create_dir_all(&root_dir)?;
        Self::sweep_stale_runs(&root_dir)?;
        let (run_dir, run_lock) = Self::create_run_dir_with_lock(&root_dir)?;
        Ok(Self {
            root_dir,
            run_dir,
            run_lock: Some(run_lock),
            next_file_id: AtomicU64::new(0),
        })
    }

    pub(super) fn read_stdin_into_mmap(&self) -> Result<InputData, Error> {
        let next_id = self.next_file_id.fetch_add(1, Ordering::Relaxed);
        let stdin_file_path = self.run_dir.join(format!("stdin-{next_id}.bin"));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&stdin_file_path)?;
        {
            let mut stdin = io::stdin().lock();
            io::copy(&mut stdin, &mut file)?;
        }
        let len = file.metadata()?.len();
        if len == 0 {
            drop(file);
            let _ = fs::remove_file(stdin_file_path);
            return Ok(InputData::Owned(String::new()));
        }
        file.flush()?;
        // SAFETY: the file remains open for the lifetime of this function call; the returned
        // mapping owns the OS mapping handle and is read-only.
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Ok(InputData::Mapped(mmap))
    }

    pub(super) fn sweep_stale_runs(root_dir: &Path) -> Result<(), Error> {
        let cleanup_lock_path = root_dir.join("cleanup.lock");
        let cleanup_lock = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(cleanup_lock_path)?;
        if cleanup_lock.try_lock_exclusive().is_err() {
            return Ok(());
        }

        for entry in fs::read_dir(root_dir)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with("run-") {
                continue;
            }

            let run_dir = entry.path();
            if parse_run_dir_pid(&run_dir).is_some_and(process_is_alive) {
                continue;
            }
            let run_lock_path = run_dir.join("run.lock");
            let run_lock = match fs::OpenOptions::new()
                .create(false)
                .read(true)
                .write(true)
                .open(&run_lock_path)
            {
                Ok(file) => file,
                Err(_) => continue,
            };

            if run_lock.try_lock_exclusive().is_ok() {
                let _ = run_lock.unlock();
                let _ = remove_spool_run_dir_if_safe(root_dir, &run_dir);
            }
        }

        let _ = cleanup_lock.unlock();
        Ok(())
    }

    fn create_run_dir_with_lock(root_dir: &Path) -> Result<(PathBuf, File), Error> {
        let pid = std::process::id();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for attempt in 0..64u32 {
            let run_dir = root_dir.join(format!("run-{pid}-{now}-{attempt}"));
            match fs::create_dir(&run_dir) {
                Ok(()) => {
                    let run_lock_path = run_dir.join("run.lock");
                    let run_lock = fs::OpenOptions::new()
                        .create_new(true)
                        .read(true)
                        .write(true)
                        .open(run_lock_path)?;
                    match run_lock.try_lock_exclusive() {
                        Ok(()) => return Ok((run_dir, run_lock)),
                        // Some filesystems don't support advisory file locking and return
                        // EINVAL/ENOTSUP/EACCES; keep the run dir usable without locking.
                        Err(err) if is_nonfatal_lock_error(&err) => return Ok((run_dir, run_lock)),
                        Err(err) => return Err(err.into()),
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        }
        Err(Error::Query(
            "failed to allocate run spool directory".to_string(),
        ))
    }
}

impl Drop for SpoolManager {
    fn drop(&mut self) {
        if let Some(run_lock) = self.run_lock.take() {
            let _ = run_lock.unlock();
            drop(run_lock);
        }
        let _ = remove_spool_run_dir_if_safe(&self.root_dir, &self.run_dir);
    }
}

fn is_nonfatal_lock_error(err: &io::Error) -> bool {
    if matches!(
        err.kind(),
        io::ErrorKind::InvalidInput
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::Unsupported
            | io::ErrorKind::WouldBlock
    ) {
        return true;
    }

    match err.raw_os_error() {
        Some(code) => is_nonfatal_lock_errno(code),
        None => false,
    }
}

#[cfg(unix)]
fn is_nonfatal_lock_errno(code: i32) -> bool {
    matches!(
        code,
        libc::EINVAL | libc::ENOTSUP | libc::EOPNOTSUPP | libc::EACCES | libc::EAGAIN
    )
}

#[cfg(not(unix))]
fn is_nonfatal_lock_errno(_code: i32) -> bool {
    false
}

fn parse_run_dir_pid(run_dir: &Path) -> Option<u32> {
    let name = run_dir.file_name()?.to_str()?;
    let suffix = name.strip_prefix("run-")?;
    suffix.split('-').next()?.parse::<u32>().ok()
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 performs permission/liveness probe only.
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    matches!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM))
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    false
}

pub(super) fn resolve_spool_root_dir() -> PathBuf {
    let base = std::env::var("ZQ_SPOOL_DIR")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("zq-spool"));
    base.join("v1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_error_classification_marks_common_nonfatal_kinds() {
        for kind in [
            io::ErrorKind::InvalidInput,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::Unsupported,
            io::ErrorKind::WouldBlock,
        ] {
            let err = io::Error::new(kind, "nonfatal");
            assert!(is_nonfatal_lock_error(&err), "{kind:?} must be nonfatal");
        }
    }

    #[test]
    fn lock_error_classification_keeps_unrelated_kinds_fatal() {
        for kind in [io::ErrorKind::NotFound, io::ErrorKind::AlreadyExists] {
            let err = io::Error::new(kind, "fatal");
            assert!(!is_nonfatal_lock_error(&err), "{kind:?} must remain fatal");
        }
    }

    #[cfg(unix)]
    #[test]
    fn lock_error_classification_accepts_common_errno_forms() {
        for code in [
            libc::EINVAL,
            libc::ENOTSUP,
            libc::EOPNOTSUPP,
            libc::EACCES,
            libc::EAGAIN,
        ] {
            let err = io::Error::from_raw_os_error(code);
            assert!(is_nonfatal_lock_error(&err), "errno={code} must be nonfatal");
        }
    }

    #[test]
    fn parse_run_dir_pid_extracts_expected_prefix() {
        let path = PathBuf::from(format!("run-{}-123-0", std::process::id()));
        assert_eq!(parse_run_dir_pid(&path), Some(std::process::id()));
        assert_eq!(parse_run_dir_pid(Path::new("run-stale")), None);
    }
}

pub(super) fn remove_spool_run_dir_if_safe(root_dir: &Path, run_dir: &Path) -> io::Result<()> {
    if !run_dir.exists() {
        return Ok(());
    }
    let canonical_root = root_dir.canonicalize()?;
    let canonical_run = run_dir.canonicalize()?;
    if canonical_run.starts_with(&canonical_root) {
        fs::remove_dir_all(canonical_run)?;
    }
    Ok(())
}
