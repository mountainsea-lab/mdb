# P38 Query API Production Hardening Design

Date: 2026-06-08
Branch: `mdb-mqdev`
Status: approved design

## Goal

P38 turns the existing market-data query route into a production-ready read API while keeping the scope intentionally narrow:

```text
HTTP query params
→ validated query request
→ server-owned query service
→ existing market-data store/query abstraction
→ deterministic response envelope with metadata
```

The goal is not to broaden the product surface with every market-data type. The goal is to harden the query contract that P37 proved end to end, so operators and clients can rely on safe, bounded, predictable trade reads from tier-backed storage.

## Current Context

The current production server already exposes:

- `GET /market-data/trades`
- optional `symbol` query parameter
- optional `limit` query parameter
- `query_trades(state, symbol, limit)` in `fdc-server` service code
- a response with `returned_records` and `records`
- readback from the server market-data store through the existing query abstraction

P37 added deterministic acceptance coverage showing fixture/live-like trade records can be persisted to tiered storage, reopened, and returned through `/market-data/trades`.

P38 builds on that result by making the query route safer and more explicit. It should not introduce new acquisition behavior, storage maintenance behavior, or broad route expansion.

## Scope

P38 includes:

- Production hardening for `GET /market-data/trades`.
- Explicit query parameter validation for `symbol` and `limit`.
- A bounded default limit and bounded maximum limit.
- Stable response metadata describing the effective query.
- Contract tests for supported query shapes and failure modes.
- Regression tests proving the query route remains read-only around live controls and storage maintenance.
- A small internal query response shape that can be extended later for candles/OHLCV without changing the core safety model.

P38 excludes:

- New `/market-data/candles` or OHLCV routes.
- New storage engine semantics.
- Network-backed live smoke in default tests.
- Acquisition, live resume, scheduler resume/reset, or maintenance execution triggered by query routes.
- Broad pagination mechanisms beyond bounded `limit`, unless implementation reveals that the existing store already has a safe cursor primitive. P38 should not invent cursor persistence.

## Recommended Approach

Use an extensible query contract and implement it only for trades in P38.

This is better than only patching the current `limit` behavior because clients need a stable response contract, not just safer internals.

This is better than adding candles/OHLCV now because the route semantics, metadata, and validation should stabilize first. Candle query support can reuse the same contract after its storage and payload expectations are explicit.

This is better than a generic `/market-data/query` endpoint because the current service already has a narrow route, and production hardening should avoid adding ambiguous public API surface.

## Public API Contract

### Route

```text
GET /market-data/trades
```

Supported query parameters:

- `symbol`: optional market symbol filter.
- `limit`: optional maximum number of records to return.

Validation rules:

- `symbol` is optional.
- If present, `symbol` must be non-empty after trimming whitespace.
- If present, `symbol` should be normalized consistently with existing stored symbols. The preferred normalization is uppercase ASCII after trimming.
- `limit` is optional.
- If absent, use a server-owned default limit.
- If present, it must be greater than zero.
- If present above the server-owned maximum, reject the request with HTTP 400 instead of silently expanding or truncating an unsafe request.

Recommended constants:

```text
DEFAULT_QUERY_LIMIT = 100
MAX_QUERY_LIMIT = 1000
```

The exact constant names may follow existing `fdc-server` style.

### Success Response

Keep the existing server envelope:

```json
{
  "status": "success",
  "message": null,
  "data": {
    "returned_records": 2,
    "requested_limit": 10,
    "applied_limit": 10,
    "symbol": "BTCUSDT",
    "data_kind": "trade",
    "query_source": "market_data_store",
    "records": []
  }
}
```

Required metadata:

- `returned_records`: number of returned records.
- `requested_limit`: query limit supplied by client, or `null` if omitted.
- `applied_limit`: final server-applied limit.
- `symbol`: normalized symbol filter, or `null` for all symbols.
- `data_kind`: `trade`.
- `query_source`: stable hint that records came through the market-data store/query abstraction.

Optional metadata if already available without boundary changes:

- `tier_hint`: coarse tier/source hint, for example `tiered` or `memory`, if it can be derived from server runtime state without querying or mutating storage internals.

P38 should not expose internal file paths, storage implementation details, or maintenance state in query responses.

### Error Response

Invalid query input returns HTTP 400 with the existing response envelope style:

```json
{
  "status": "error",
  "message": "limit must be between 1 and 1000",
  "data": {
    "returned_records": 0,
    "requested_limit": 0,
    "applied_limit": 100,
    "symbol": null,
    "data_kind": "trade",
    "query_source": "market_data_store",
    "records": []
  }
}
```

The error response should be deterministic and should not include raw parser errors, stack traces, tier paths, or secrets.

## Internal Design

### Query parameter parsing

`TradeQueryParams` should remain the router-level deserialization type, but validation should be explicit before calling the query service.

Recommended shape:

```text
TradeQueryParams
→ validate_trade_query_params(params)
→ ValidatedTradeQuery
→ query_trades(state, validated)
```

`ValidatedTradeQuery` should contain:

- `symbol: Option<String>` after trimming/normalization.
- `requested_limit: Option<usize>`.
- `applied_limit: usize`.

The validation function should return a small query error type or message that the router can convert to HTTP 400.

### Query service

`query_trades` should accept the validated query shape or equivalent individual validated fields. It should not perform HTTP-specific work, but it may build response metadata because that metadata is part of the server query contract.

The service should continue to use the existing market-data query/store abstraction:

```text
MarketDataQuery::for_trades()
→ optional symbol filter
→ applied limit
→ state.market_data_store().query(&query)
→ record_to_trade_record
→ MarketDataTradesResponse
```

No P38 code should call live start/resume/stop, storage maintenance run-once, scheduler reset/resume, or audit reset from the query path.

### Extensibility for candles/OHLCV

P38 should keep the trade query design easy to reuse, but not implement candles yet.

Acceptable extension points:

- neutral naming such as `data_kind` in metadata;
- reusable limit validation constants/helper;
- small validated query structs that could later be mirrored by candle-specific structs;
- tests that avoid trade-only assumptions in shared validation helpers.

Do not add placeholder routes, dead code, or incomplete candle response types.

## Data Flow

```text
Client
  GET /market-data/trades?symbol=btcusdt&limit=10
    ↓
Router deserializes TradeQueryParams
    ↓
Router validates and normalizes params
    ↓
Service builds MarketDataQuery::for_trades()
    ↓
Market-data store returns records
    ↓
Service maps storage records to trade response records
    ↓
Router returns success envelope with metadata
```

For invalid input:

```text
Client
  GET /market-data/trades?limit=0
    ↓
Router deserializes TradeQueryParams
    ↓
Validation fails
    ↓
Router returns HTTP 400 error envelope
    ↓
No store query, acquisition, live control, or maintenance action occurs
```

## Contract Tests

Primary test file:

- `crates/fdc-server/tests/production_server_router_contract.rs`

Recommended P38 tests:

1. `p38_trades_query_applies_default_limit_and_metadata`
   - Insert more fixture records than the default or use a smaller test override if supported.
   - Query without `limit`.
   - Assert `applied_limit` is the default, `requested_limit` is null, `data_kind` is `trade`, and returned count is bounded.

2. `p38_trades_query_filters_normalized_symbol`
   - Insert records for multiple symbols.
   - Query with lowercase or padded symbol if the HTTP client can encode it safely.
   - Assert response symbol is normalized and returned records match only that symbol.

3. `p38_trades_query_returns_empty_success_for_missing_symbol`
   - Query a valid symbol that has no records.
   - Assert HTTP 200, success envelope, zero records, and correct metadata.

4. `p38_trades_query_rejects_invalid_limits`
   - Query `limit=0` and `limit=MAX_QUERY_LIMIT + 1`.
   - Assert HTTP 400, error envelope, sanitized message, and zero records.

5. `p38_trades_query_survives_durable_reopen_with_metadata`
   - Reuse P37-style tiered durable setup.
   - Insert fixture trades, drop/reopen state, query route.
   - Assert metadata and records remain correct.

6. `p38_trades_query_is_read_only_for_operational_controls`
   - Capture record count/query snapshot.
   - Exercise maintenance run-once and safe live resume failure/conflict paths already covered by P37/P36 helpers.
   - Query again and assert route did not trigger or hide mutations beyond existing maintenance semantics.

Existing P36/P37 regression tests should remain green.

## Error Handling and Safety

P38 query validation must fail closed for invalid input. Invalid input should not reach the storage query path.

Error messages should be useful but sanitized. They may name invalid fields and allowed ranges, but must not expose internals.

Queries must be bounded. There should be no unbounded all-record HTTP response by default.

Queries must remain read-only. The query route must not:

- start acquisition;
- resume live collection;
- stop live collection;
- run storage maintenance;
- reset maintenance audit;
- reset or resume the scheduler;
- alter tier paths;
- clear records.

## Files Expected to Change

Likely implementation files:

- `crates/fdc-server/src/market_data/router.rs`
- `crates/fdc-server/src/market_data/service.rs`
- `crates/fdc-server/src/market_data/model.rs`
- `crates/fdc-server/tests/production_server_router_contract.rs`

Possible documentation updates:

- `docs/DEVELOPMENT_STATUS.md`

No `fdc-storage` source files should change unless P38 reveals a genuine query boundary bug. If such a bug appears, it should be fixed with a focused storage test and documented explicitly.

## Verification Plan

Focused verification should include:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract p38_
rtk cargo test -p fdc-server --test production_server_router_contract p37_
rtk cargo test -p fdc-server --test production_server_router_contract production_live_resume
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_resume
rtk cargo test -p fdc-storage --test dependency_guard
rtk cargo fmt -p fdc-server -p fdc-storage -- --check
```

If P38 changes shared model serialization, add the narrowest relevant model/contract tests for that serialization.

## Completion Criteria

P38 is complete when:

- `/market-data/trades` has explicit validation for supported query params.
- The route applies bounded defaults and rejects invalid limits with HTTP 400.
- The response includes stable query metadata.
- Empty results are successful and deterministic.
- Tiered/durable readback still works after reopen.
- Query routes are proven read-only against storage maintenance and live control regressions.
- Focused P38 tests pass.
- P37, live resume, storage maintenance, scheduler resume, storage dependency guard, and formatting regressions pass.
- `docs/DEVELOPMENT_STATUS.md` records P38 completed work, commits, and verification results.
- The next recommended slice remains P39 Production Runbook, Config Pack, and Soak Validation.
