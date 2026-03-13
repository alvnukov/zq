# Release Process

## Versioning

- Semantic versioning (MAJOR.MINOR.PATCH)
- Tag format: `vX.Y.Z`

## Checklist

1. Ensure `Cargo.toml` version is final.
2. Curate `CHANGELOG.md`:
   - move completed `Unreleased` entries into `## [X.Y.Z] - YYYY-MM-DD`;
   - keep only user-visible changes;
   - drop local-only maintenance noise.
3. Ensure all CI jobs pass on `main`.
4. Create and push tag `vX.Y.Z`.
5. Verify release workflow artifacts and checksums.
6. Verify Homebrew tap update.

## Rollback

- If release is broken, publish a patch release with fixes.
- Do not rewrite released tags in normal operation.
