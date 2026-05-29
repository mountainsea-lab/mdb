# Post-MVP Stabilization Baseline

This document is the post-MVP stabilization baseline after acceptance of the first internal MVP. It records current technical debt and follow-up tracks without changing runtime behavior.

## Accepted MVP reference

The accepted MVP is documented here:

- Acceptance report: `docs/mvp/first-mvp-acceptance-report.md`
- Demo guide: `docs/mvp/first-mvp-demo.md`
- Development status: `docs/DEVELOPMENT_STATUS.md`

The accepted MVP remains a no-listener, in-memory, deterministic market-data demo.

## Warning baseline

Baseline command:

```bash
CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api
```

Baseline result at B20a start:

```text
0 errors, 28 warnings
```

The 28 warnings are in the `fdc-api` dependency graph, not necessarily in `fdc-api` itself.

Affected crates observed in the baseline:

- `fdc-wasm`
- `fdc-types`
- `fdc-storage`
- `fdc-query`
- `fdc-ingestion`

Warning classes observed:

- unused imports
- unused variables
- unnecessary `mut`
- dead fields in older manager/runtime structs

B20a records this baseline only. It does not perform broad warning cleanup because warnings cross multiple older crates and should be handled in narrow, reviewable slices.

## Cargo.lock policy baseline

`Cargo.lock` exists locally at the workspace root, but `.gitignore` currently ignores `Cargo.lock`.

B20a does not change this policy. The decision should be made in a dedicated follow-up because the repository currently has an application-like workspace with reproducibility benefits from tracking lockfiles, but it also has existing history and ignore behavior that should be changed deliberately.

Recommended follow-up: B20c Cargo.lock policy decision.

## Dependency boundary baseline

The post-MVP dependency boundary remains:

- lower-level crates must not depend on `fdc-api`.
- adapter, ingestion, transform, storage, orchestrator, server, and API boundaries should remain explicit.
- cross-layer market-data glue belongs in orchestration or API/demo assembly, not in lower-level core crates.

Existing MVP contract tests include dependency boundary checks for relevant API-facing slices. B20a does not add a broad workspace dependency graph tool yet.

Recommended follow-up: B20d dependency boundary audit report.

## Stabilization follow-up tracks

Recommended next stabilization slices:

1. B20b warning cleanup by crate: pick one crate at a time, remove unused imports/variables where behavior is unaffected, and run that crate's tests.
2. B20c Cargo.lock policy decision: decide whether to track `Cargo.lock`, update `.gitignore`, and document the rationale.
3. B20d dependency boundary audit report: centralize dependency-boundary checks and document allowed edges.

## Non-goals for B20a

B20a does not:

- edit runtime code.
- fix all 28 warnings.
- change `.gitignore`.
- start tracking or deleting `Cargo.lock`.
- add production persistence, SQL integration, auth, or HTTP demo startup.
- turn warnings into hard errors.

## Verification

Run:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test stabilization_baseline_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test mvp_acceptance_report_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api
```
