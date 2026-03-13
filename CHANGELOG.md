# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.5.1] - 2026-03-13

### Changed
- Native JSON fast-path now prunes unused root fields and reduces intermediate materialization on common jq-style filters, projections, and comparisons.

### Fixed
- Release and CI workflows now use pinned Node 24-compatible actions, a pinned `jq 1.7.1` upstream compatibility baseline, and tag-only release publishing.
- CI lint jobs no longer trip on platform-specific errno aliases or recycle-path clippy warnings.

## [1.5.0] - 2026-03-12

### Added
- Tape-backed JSON fast-path execution for common jq-style filters, projections, comparisons, and literal `test("...")` predicates.
- Governance, contribution, and security policy documents.
- Broader jq compatibility coverage across upstream, parity, and shell-based suites.

### Changed
- CI and development workflow documentation aligned with the current release and verification flow.
- Short-filter execution reduces materialization and allocation overhead on the native JSON path.

### Fixed
- Arbitrary-precision JSON numbers now deserialize correctly on streaming input paths.
- UTF-8 and user-facing compatibility regressions in CLI/runtime edge cases.

## [1.4.2] - 2026-03-09

### Fixed
- UTF-8 edge-case stability regressions.
- YAML merge and anchor behavior fixes.

### Added
- Diff patch output mode for semantic diff workflows.

## [1.4.0] - 2026-03-09

### Added
- Streaming-focused runtime improvements.
- Broader format support and compatibility hardening.

## [1.3.0] - 2026-03-08

### Added
- Expanded compatibility and test coverage baseline.

[Unreleased]: https://github.com/alvnukov/zq/compare/v1.5.1...HEAD
[1.5.1]: https://github.com/alvnukov/zq/releases/tag/v1.5.1
[1.5.0]: https://github.com/alvnukov/zq/releases/tag/v1.5.0
[1.4.2]: https://github.com/alvnukov/zq/releases/tag/v1.4.2
[1.4.0]: https://github.com/alvnukov/zq/releases/tag/v1.4.0
[1.3.0]: https://github.com/alvnukov/zq/releases/tag/v1.3.0
