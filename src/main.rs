mod cli;
mod service;

use std::io::Write;
use std::process;

fn main() {
    zq::initialize_process_locale_from_env();
    match service::run() {
        Ok(code) => process::exit(code),
        Err(service::Error::Query(msg)) => {
            if let Some((code, stderr)) = decode_halt_error_from_message(&msg) {
                let mut handle = std::io::stderr().lock();
                let _ = handle.write_all(stderr.as_bytes());
                let _ = handle.flush();
                process::exit(code);
            }
            eprintln!("{msg}");
            let is_compile_error = msg.starts_with("zq: unsupported query:")
                || msg.contains("zq: 1 compile error")
                || msg.starts_with("jq: unsupported query:")
                || msg.contains("jq: 1 compile error");
            process::exit(if is_compile_error { 3 } else { 5 });
        }
        Err(service::Error::Io(err)) => {
            let tool = service::cli_error_tool_name();
            eprintln!("{tool}: error: {err}");
            process::exit(2);
        }
    };
}

fn decode_halt_error_from_message(msg: &str) -> Option<(i32, String)> {
    let idx = msg.find("\u{1f}zq-halt:")?;
    zq::decode_native_halt_error(&msg[idx..])
}
