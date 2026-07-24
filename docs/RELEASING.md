# Releasing Truffle

Truffle uses Release Please as its only versioning and changelog system. Do
not run Changesets or manually publish an individual package: the JavaScript
packages, Rust crates, native addons, sidecars, lockfile, and checksum maps
form one release unit.

## Release contract

Every public artifact has the same version. `scripts/check-release-versions.mjs`
enforces that invariant across package manifests, Cargo manifests, and
`Cargo.lock`.

Generated lockfiles are part of that contract. Run `pnpm release:prepare`
after changing release versions; `pnpm release:check` fails if either
`Cargo.lock` or `crates/truffle-napi/package-lock.json` is stale. Release
Please performs this preparation on its PR and explicitly dispatches the CI
workflows because a bot-token push does not trigger them automatically.

Sidecar binaries are fail-closed:

- A supported platform must have a SHA-256 checksum for the exact release
  version.
- A missing or mismatched checksum fails installation/build.
- PATH fallback is allowed only through the explicit
  `TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1` development escape hatch or an explicit
  `TRUFFLE_SIDECAR_PATH`.

The source tag is immutable. Generated checksum maps are committed back to
the default branch only after every sidecar asset exists and is hashed.

## Normal release

1. Merge conventional commits to `main`.
2. Wait for Release Please to synchronize the generated lockfiles and
   dispatch CI on its PR branch. Review the PR only after those runs are
   green. The pre-tag graph is validated with:

   ```bash
   pnpm release:check
   ```

   The checksum exception is intentional here: binaries for the new version
   cannot exist before its tag. This accepts only a version that is completely
   absent from both checksum maps; a partial, malformed, or mismatched checksum
   set still fails CI.

3. Merge the Release Please PR. Release Please creates an immutable
   `truffle-v<version>` tag and a **draft** GitHub release. It waits for CI,
   real-network integration tests, and CodeQL on that exact commit before
   dispatching the CLI, sidecar, and NAPI workflows.
4. The sidecar workflow builds all five targets, attaches them to the GitHub
   release, verifies their hashes, publishes the platform sidecar packages,
   and commits identical checksum maps for Rust and JavaScript consumers.
5. Only after step 4 succeeds, the sidecar workflow dispatches the Rust-crate
   and primary npm-package publishers. Both run strict validation:

   ```bash
   node scripts/check-release-versions.mjs
   ```

6. The orchestrator publishes the draft GitHub release only after every
   binary build and package publisher succeeds. Confirm the GitHub release
   and all registries show the same version:
   crates.io (`truffle`, `truffle-core`, `truffle-sidecar`),
   npm (`@vibecook/truffle`, `@vibecook/truffle-react`,
   `@vibecook/truffle-native`, and all platform packages), and the CLI
   archives.

## Reruns and failures

Workflow reruns are safe. The crate publisher checks the exact version through
the crates.io sparse index, waits for each dependency to become visible there,
and skips only versions that are already present. Registry lookup failures and
real `cargo publish` errors still fail the job. npm's provenance-enabled
publish steps likewise skip only an exact version already in the registry.

If a release job fails, the GitHub release remains a draft. Do not move or
replace its tag, and do not bypass the preflight by dispatching a downstream
publisher without its preflight. Correct the automation on `main`; the crate
and primary npm workflows accept a `release_tag` input so their corrected
workflow definitions can resume a draft release from `main` while checking out
and verifying the unchanged immutable tag.

If a registry has only part of a release, rerun the relevant workflow at the
same tag. Never change a released artifact or reuse a version number.

## Pre-release verification

Before merging a release PR:

```bash
pnpm release:check
pnpm release:tooling:test
pnpm install --frozen-lockfile --ignore-scripts
pnpm run build
pnpm run test
cargo fmt --all -- --check
TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 cargo clippy --locked --workspace --all-targets \
  --exclude truffle-tauri-plugin --exclude truffle-napi -- -D warnings
cargo test --locked --workspace
```

The real-tailnet workflow and the Linux ignored transport suite must also be
green. See [TESTING.md](TESTING.md) for credentials and local commands.
