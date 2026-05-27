# B10 Bounded API Market Data Query Route Design

## Goal

Expose the B9 queryable market-data store through one bounded API route so the first MVP can demonstrate data acquisition -> storage -> API query without starting real network listeners.

## Scope

In scope:

- Extend `fdc-api` with access to a shared `QueryableMarketDataStore`.
- Add API DTOs for market-data trade query responses.
- Add a pure query helper that reads `StorageWriteRecord` values from B9 storage and converts them into API response records.
- Add an Axum router factory for one in-memory route: `GET /market-data/trades?symbol=BTCUSDT&limit=10`.
- Test the route using `tower::ServiceExt` without binding sockets.

Out of scope:

- Starting HTTP listeners.
- Live runner lifecycle.
- Production persistence.
- SQL query engine integration.
- Moving Barter/orchestrator mapping logic into `fdc-api`.
- Full REST API cleanup or broad router refactor.

## Current Context

B8 added `ApiAppState` around `FdcServerApp` and readiness projection helpers.

B9 added:

- `fdc_storage::QueryableMarketDataStore`
- `fdc_storage::MarketDataQuery`
- A proven path from Barter fixture envelopes through `fdc-orchestrator` into queryable storage.

B10 uses B9 as a read boundary. `fdc-api` may depend on `fdc-storage` as a consumer, but lower-level crates must not depend on `fdc-api`.

## Design Summary

B10 adds a small API market-data module. The module has no acquisition or transformation logic. It only translates query parameters into `MarketDataQuery`, executes the query against the shared B9 store, and returns storage-backed API DTOs.

`ApiAppState` gains a shared `Arc<QueryableMarketDataStore>` alongside the existing `Arc<FdcServerApp>`. Default construction uses an empty store, while tests and server assembly can inject a seeded store.

The route is built by a focused router factory and tested in memory. This keeps the route useful for MVP validation while avoiding real network services.

## Components

### `ApiAppState` extension

Add to `crates/fdc-api/src/state.rs`:

- `market_data_store: Arc<QueryableMarketDataStore>`
- `with_market_data_store(self, Arc<QueryableMarketDataStore>) -> Self`
- `market_data_store(&self) -> Arc<QueryableMarketDataStore>`

Existing B8 readiness behavior remains unchanged.

### `market_data` module

Create `crates/fdc-api/src/market_data.rs`.

Public types:

- `MarketDataTradeQueryParams`
  - `symbol: Option<String>`
  - `limit: Option<usize>`
- `MarketDataTradeRecord`
  - `key: String`
  - `symbol: Option<String>`
  - `kind: Option<String>`
  - `source: Option<String>`
  - `payload: serde_json::Value`
- `MarketDataTradesResponse`
  - `records: Vec<MarketDataTradeRecord>`
  - `returned_records: usize`

Public functions:

- `query_market_data_trades(state, params) -> ApiResponse<MarketDataTradesResponse>`
- `build_market_data_router(state) -> axum::Router`

Route:

- `GET /market-data/trades`
- Query params:
  - `symbol`, optional. If omitted, returns all trade records subject to limit.
  - `limit`, optional. If omitted, uses default 100. If `0`, returns empty list.

## Response Shape

Example:

```json
{
  "status": "success",
  "message": null,
  "data": {
    "records": [
      {
        "key": "barter:binance_spot:BTCUSDT:trade:1700000000000000001",
        "symbol": "BTCUSDT",
        "kind": "trade",
        "source": "barter:binance_spot",
        "payload": { "event_id": "env-1", "symbol": "BTCUSDT" }
      }
    ],
    "returned_records": 1
  }
}
```

The existing `ApiResponse<T>` also includes `timestamp`, `request_id`, and optional `metadata`.

## Error Handling

- Invalid JSON payloads are represented as `serde_json::Value::Null` for B10, not route failures. Storage writes should normally come from orchestrator and already contain valid JSON.
- Missing symbol filter is allowed.
- `limit=0` returns no records.

## Testing Strategy

Add `crates/fdc-api/tests/market_data_route_contract.rs` with tests that:

1. Seed a `QueryableMarketDataStore` with BTCUSDT and ETHUSDT trade records and verify the pure helper returns only BTCUSDT for `symbol=BTCUSDT`.
2. Verify `limit=1` returns one record.
3. Build the in-memory router and call `GET /market-data/trades?symbol=BTCUSDT&limit=10` with `tower::ServiceExt`; assert HTTP 200 and response JSON contains one BTCUSDT record.
4. Verify dependency guard: lower-level crates do not reference `fdc-api`.

## Acceptance Criteria

- Given a seeded B9 store with a BTCUSDT trade, when calling the pure helper with `symbol=BTCUSDT`, then the API response contains exactly one BTCUSDT record.
- Given two matching trades and `limit=1`, then the response contains one record.
- Given an in-memory Axum router and query request `/market-data/trades?symbol=BTCUSDT&limit=10`, then the response status is 200 and JSON data contains the seeded trade payload.
- No real listener is started during tests.
- Lower-level crates do not depend on `fdc-api`.

## Next Slice

B11 should decide how to populate the shared store from a bounded acquisition runner: either a manual fixture ingestion command or a limited live acquisition runner that collects N Binance trades and writes them through orchestrator.
