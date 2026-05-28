# B11 Bounded Acquisition-to-API MVP Runner Design

## Status

Approved direction: fixture-only bounded runner for the next development slice.

## Context

The project has completed the first bounded market-data path slices:

- B6 maps `BarterIngestionEnvelope` values through orchestration into storage write records.
- B9 adds `QueryableMarketDataStore`, an in-memory `StorageWriteSink` that can be queried by symbol and market-data kind.
- B10 exposes a bounded in-memory API route, `GET /market-data/trades`, backed by an injected `QueryableMarketDataStore`.

B11 should connect these existing pieces into one repeatable MVP flow without introducing production service lifecycle, persistence, or default network tests.

## Goal

Demonstrate a controlled acquisition -> orchestration -> storage -> API query path:

1. Accept a finite set of Barter acquisition envelopes.
2. Write them through the existing orchestrator helper into a shared `QueryableMarketDataStore`.
3. Build API state around that same store.
4. Query the stored trades through the B10 in-memory route.

## Non-Goals

B11 does not add:

- Long-running service lifecycle or background tasks.
- Production persistence or tier-aware runtime routing.
- SQL engine integration.
- Ungated network tests.
- New adapter, transform, ingestion, or storage coupling.
- A dependency from `fdc-orchestrator` to `fdc-api`.

## Recommended Approach

Use a fixture-only bounded MVP runner in `fdc-server`, because `fdc-server` is already the application assembly boundary and can depend on both `fdc-orchestrator` and `fdc-storage` without reversing lower-level dependency direction.

The runner should be small and explicit:

- `run_barter_fixture_mvp_once(envelopes, app)` or equivalent accepts finite Barter envelopes plus a server/app assembly handle that owns a shared queryable market-data store.
- The helper invokes `fdc_orchestrator::pipeline::run_barter_envelopes_to_storage_once` with the shared `QueryableMarketDataStore`.
- The helper returns a compact result with counts needed by tests and future demos, such as envelopes accepted and storage records written.
- API tests can then build `ApiAppState` from the same server app/store and call `build_market_data_router`.

This avoids making lower-level crates aware of API concerns while giving the MVP one clear assembly-level entry point.

## Component Boundaries

### `fdc-server`

Owns the B11 bounded runner because it is the assembly layer. It may coordinate server app state, the queryable store, and orchestrator helpers.

### `fdc-orchestrator`

Continues to own concrete Barter -> ingestion -> transform -> storage mapping. B11 should reuse the existing finite helper rather than duplicating mapping logic.

### `fdc-storage`

Continues to own `QueryableMarketDataStore` and storage query types. It must not depend on API, server, orchestrator, transform, ingestion, or adapter crates.

### `fdc-api`

Continues to own pure API projection and the bounded Axum route. It should not acquire data itself and should only read from an injected shared store.

## Data Flow

```text
Vec<BarterIngestionEnvelope>
  -> fdc-server bounded MVP runner
  -> fdc-orchestrator::run_barter_envelopes_to_storage_once
  -> shared fdc-storage::QueryableMarketDataStore
  -> fdc-api::ApiAppState::with_market_data_store
  -> fdc-api::build_market_data_router
  -> GET /market-data/trades?symbol=BTCUSDT&limit=N
```

## Error Handling

- Invalid envelopes or storage write failures should propagate from the existing orchestrator helper.
- The bounded runner should not swallow errors or convert them into HTTP responses.
- API route behavior remains B10-owned: successful queries return a JSON success response, including empty result sets when no records match.

## Testing Strategy

Add contract coverage that proves the end-to-end bounded path works offline:

1. Build one or more deterministic `BarterIngestionEnvelope` fixtures.
2. Run the B11 bounded runner into a shared `QueryableMarketDataStore`.
3. Assert the runner reports the expected storage write count.
4. Build API state with the same store.
5. Call `GET /market-data/trades` through the in-memory Axum router.
6. Assert the response contains the expected trade payload and filtering behavior.
7. Keep dependency guard coverage so lower-level crates do not reference `fdc-api` or `fdc-server` unexpectedly.

## Verification

Expected local verification for this slice:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-server --package fdc-api --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server --test acquisition_api_mvp_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test market_data_route_contract
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-server -p fdc-api -p fdc-orchestrator -p fdc-storage
```

## Future Work

After B11, later slices can separately add:

- An ignored, network-gated live trade smoke test.
- A real application runner lifecycle with cancellation and readiness transitions.
- Production storage runtime routing and persistence.
- SQL-backed market-data querying.
