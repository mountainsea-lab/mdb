# B19 MVP Acceptance Report Design

## Status

Approved direction: create a documentation-first MVP acceptance report that declares the first internal MVP complete, freezes scope, and lists verification evidence.

## Context

B18 added `docs/mvp/first-mvp-demo.md`, which explains how to reproduce the no-listener first MVP demo. The next useful step is to produce a concise acceptance report that answers:

- What is accepted as the first internal MVP?
- Which capabilities are included?
- Which production/platform capabilities are explicitly excluded?
- What commands prove the MVP still works?
- What should happen after this MVP is accepted?

This report is not a new runtime feature. It is a checkpoint artifact for development continuity and scope control.

## Goal

Add `docs/mvp/first-mvp-acceptance-report.md` that:

1. Declares the first internal MVP accepted.
2. Defines the accepted MVP scope.
3. Summarizes completed phases B1-B18 at capability level.
4. Lists required verification commands and expected pass counts.
5. Links to the first MVP demo guide.
6. Freezes non-goals for the accepted MVP.
7. Identifies recommended post-MVP next steps.

## Non-Goals

B19 does not add:

- Runtime code.
- HTTP demo binary or listener startup.
- New tests for market-data behavior beyond documentation contracts.
- Production persistence, SQL integration, authentication, or daemon supervision.
- Live network acquisition in default verification.
- A performance benchmark claim.

## Recommended Approach

Create a single report at `docs/mvp/first-mvp-acceptance-report.md`.

Add a documentation contract test in `crates/fdc-api/tests/mvp_acceptance_report_contract.rs` that verifies the report remains anchored to current MVP scope and commands. The test should assert the report mentions:

- `First internal MVP: accepted`
- `docs/mvp/first-mvp-demo.md`
- `run_demo_flow_once`
- `build_demo_router`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract`
- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract`
- `no-listener`
- `in-memory`
- `no persistence`
- `SQL integration is out of scope`
- `live network acquisition is not part of default verification`
- `gated local HTTP demo entrypoint`

The contract should also reject placeholders: `TODO`, `TBD`, `FIXME`.

## Report Structure

The report should contain:

1. Acceptance decision.
2. Accepted user journey.
3. Included capabilities.
4. Verification evidence.
5. Explicit non-goals.
6. Key artifacts and links.
7. Residual risks and caveats.
8. Recommended next slices.

## Acceptance Criteria

- The report states that the first internal MVP is accepted.
- The report is clear that this is an internal, no-listener, in-memory MVP.
- The report lists exact commands that verify B16-B18 demo/router/documentation behavior.
- The report links to `docs/mvp/first-mvp-demo.md`.
- The report freezes non-goals: persistence, SQL integration, auth, production daemon supervision, and default live network acquisition.
- Documentation contract tests pass.
- No runtime code changes are required.

## Future Work

After B19, the project can move to one of these tracks:

1. Gated local HTTP demo entrypoint for interactive external demonstration.
2. Stabilization cleanup: warnings, Cargo.lock policy, dependency audit, docs polish.
3. Next capability milestone: persistence boundary, SQL query integration, or production runner lifecycle.
