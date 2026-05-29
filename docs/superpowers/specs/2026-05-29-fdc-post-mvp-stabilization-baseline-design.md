# B20a Stabilization Baseline Design

## Status

Approved direction: create a post-MVP stabilization baseline instead of immediately editing warnings across many older crates.

## Context

The first internal MVP is accepted in `docs/mvp/first-mvp-acceptance-report.md`. Before expanding product capabilities, the project needs a stabilization checkpoint that records current technical debt and prevents accidental loss of MVP boundaries.

Current exploration found:

- `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api` completes with 0 errors and 28 warnings across the `fdc-api` dependency graph.
- Warnings are spread across older crates including `fdc-wasm`, `fdc-types`, `fdc-storage`, `fdc-query`, and `fdc-ingestion`.
- `Cargo.lock` exists locally but `.gitignore` ignores it.
- `docs/DEVELOPMENT_STATUS.md` already notes that Cargo.lock policy should be revisited.

## Goal

Add a stabilization baseline report that:

1. Records the current warning baseline and affected crates.
2. Explains why B20a does not perform broad warning cleanup yet.
3. Documents the current `Cargo.lock` tracking policy and recommended future decision.
4. Re-states dependency-boundary expectations after MVP acceptance.
5. Defines recommended stabilization next steps.
6. Is protected by a documentation contract test.

## Non-Goals

B20a does not:

- Fix all warnings across the workspace.
- Change `.gitignore` or start tracking `Cargo.lock`.
- Change runtime behavior.
- Add new feature capabilities.
- Add production persistence, SQL integration, authentication, or HTTP demo startup.
- Turn warnings into hard errors.

## Recommended Approach

Create `docs/mvp/post-mvp-stabilization-baseline.md`.

The report should include:

- Accepted MVP reference and links.
- Warning baseline from `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api`.
- Warning classes: unused imports, unused variables, dead fields, unnecessary mutability.
- Affected crates.
- Cargo.lock policy note: currently ignored, exists locally, should be decided in a dedicated follow-up.
- Dependency boundary guard summary.
- Recommended follow-up tracks:
  - B20b warning cleanup by crate.
  - B20c Cargo.lock policy decision.
  - B20d dependency boundary audit report.

Add `crates/fdc-api/tests/stabilization_baseline_contract.rs` to verify the report mentions core anchors:

- `post-MVP stabilization baseline`
- `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api`
- `28 warnings`
- `fdc-wasm`
- `fdc-types`
- `fdc-storage`
- `fdc-query`
- `fdc-ingestion`
- `Cargo.lock`
- `.gitignore`
- `dependency boundary`
- `B20b warning cleanup by crate`
- `B20c Cargo.lock policy decision`
- No placeholders: `TODO`, `TBD`, `FIXME`.

## Acceptance Criteria

- `docs/mvp/post-mvp-stabilization-baseline.md` exists and clearly distinguishes baseline recording from cleanup.
- The report lists current warning count and affected crates.
- The report documents current Cargo.lock status without changing policy.
- The report links the accepted MVP artifacts.
- The report identifies concrete follow-up slices.
- Documentation contract tests pass.
- Existing MVP documentation/demo tests still pass.

## Verification

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```

## Future Work

After B20a, choose one narrow stabilization follow-up:

1. B20b warning cleanup by crate, starting with one crate and preserving behavior.
2. B20c Cargo.lock policy decision and implementation.
3. B20d dependency boundary audit report.
