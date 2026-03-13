# Contributing to zq

Thanks for contributing to `zq`.

## Ground Rules

- Keep behavior backward-compatible for CLI, library API, and documented formats.
- Prefer small, reviewable pull requests.
- Add or update tests for every behavior change.
- Do not commit generated local artifacts (`target/`, `.tmp/`, IDE files).

## Development Setup

1. Install Rust toolchain from [rust-toolchain.toml](rust-toolchain.toml).
2. Clone the repository and run:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features --locked
```

Optional for cleaner `git blame`:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

## Change Workflow

1. Create a branch from `main`.
2. Write tests first for changed behavior.
3. Implement the change.
4. Run relevant tests locally, then full test suite before opening PR.
5. Update docs (`README.md`, `docs/*`) when user-facing behavior changes.
6. Add changelog entry under `Unreleased` in [CHANGELOG.md](CHANGELOG.md).

## Changelog Rules

- Keep `CHANGELOG.md` curated for user-visible changes only.
- Add entries to `Unreleased` as part of the same change, not during release scramble.
- Prefer concrete impact over implementation detail.
- Do not add local-only noise such as coverage badge updates, formatting-only commits, or refactors with no observable effect.
- Group entries under Keep a Changelog sections (`Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`).

## Commit Messages

Use clear commit subjects, preferably conventional prefixes:

- `feat:` new functionality
- `fix:` bug fix
- `refactor:` internal refactor with no behavior change
- `test:` tests only
- `docs:` documentation only
- `ci:` workflow/pipeline changes
- `chore:` maintenance

## Pull Request Checklist

- [ ] Tests added/updated for changed logic
- [ ] `cargo fmt` and `cargo clippy` pass
- [ ] Full tests pass locally
- [ ] Public docs updated
- [ ] Changelog updated

## Reporting Issues

Use the issue templates in `.github/ISSUE_TEMPLATE`.
For security issues, do not open a public issue. See [SECURITY.md](SECURITY.md).
