# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/alvnukov/zq/compare/v1.5.0...HEAD
[1.5.0]: https://github.com/alvnukov/zq/releases/tag/v1.5.0
[1.4.2]: https://github.com/alvnukov/zq/releases/tag/v1.4.2
[1.4.0]: https://github.com/alvnukov/zq/releases/tag/v1.4.0
[1.3.0]: https://github.com/alvnukov/zq/releases/tag/v1.3.0
