# Security

For supported versions and reporting instructions, see:

- [SECURITY.md](../SECURITY.md)

## Dependency hygiene

Security checks are enforced in CI via:

- `cargo audit`
- `cargo deny check advisories`
- Dependabot updates for Cargo and GitHub Actions
