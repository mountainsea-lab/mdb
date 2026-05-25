# B4b Ingestion-Centered Transform Boundary Correction Design

Date: 2026-05-25
Branch: `mdb-mqdev`

## Intent

Correct the B4 market-data transform boundary so that adapters do not depend on `fdc-transform`. The project goal is a market-data center where many adapters flow through `fdc-ingestion` before transform and storage.

## Architecture Decision

The authoritative data flow is:

```text
adapter(s) -> fdc-ingestion -> fdc-transform -> fdc-storage
```

The crate dependency model should not simply mirror the data flow. The corrected dependencies are:

- `fdc-adapter/*` should depend only on `fdc-core` and adapter-specific external libraries for production code. Adapter crates produce adapter-owned envelopes/events and do not depend on `fdc-ingestion` or `fdc-transform`.
- `fdc-ingestion` remains generic and does not depend on adapters or transform.
- `fdc-adapter/*` should depend only on `fdc-core` and adapter-specific external libraries for production code. Adapter crates produce adapter-owned envelopes/events and do not depend on `fdc-ingestion` or `fdc-transform`.
- `fdc-storage` must remain generic and must not depend on adapter or transform DTOs.
- A later `fdc-server`, orchestration layer, or dedicated integration crate wires adapter, ingestion, transform, and storage together.
- `fdc-transform` core must not depend on specific adapter crates.

This reverses the incorrect B4 dependency `fdc-barter -> fdc-transform`.

## Scope

In this correction slice:

1. Remove `fdc-transform` and `fdc-ingestion` from `fdc-barter` production dependencies.
2. Remove `fdc-barter::IntoMarketDataDto`, adapter-local transform mapper, and adapter-local source bridge.
3. Keep only generic `MarketDataDto` and `MarketDataTransformSink` in `fdc-transform` core.
4. Defer Barter-to-`SourceEnvelope` and Barter-to-`MarketDataDto` glue to a future orchestration/integration slice.
5. Update docs and status to reflect the corrected dependency direction.

## Non-goals

- No B5 storage sink implementation.
- No real database writes.
- No server/orchestrator runtime.
- No new adapter framework crate.
- No large refactor of existing B1 source pipeline primitives.

## Acceptance Criteria

- `crates/fdc-adapter/barter/Cargo.toml` has no `fdc-ingestion` or `fdc-transform` dependency.
- `fdc-barter` no longer exports `IntoMarketDataDto`.
- `fdc-transform` owns neutral DTO and sink contracts but has no adapter-specific dependencies.
- Contract tests prove validated Barter source batches can be transformed after `fdc-ingestion` validation/batching.
- Dependency guard proves `fdc-ingestion` has no `fdc-barter` or `fdc-transform` dependency, `fdc-barter` has no `fdc-ingestion` or `fdc-transform` dependency, and `fdc-transform` has no adapter-specific dependency.
