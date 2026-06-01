# fdc-barter Multi-Exchange Historical REST Implementation Plan

Date: 2026-06-01
Module: `crates/fdc-adapter/barter`
Design: `docs/superpowers/specs/2026-06-01-fdc-barter-multi-exchange-historical-rest-design.md`

## Goal

Implement the first offline-testable slice of a Barter-integration-inspired historical REST acquisition boundary for `fdc-barter`.

## Constraints

- Modify only `fdc-barter` and development documentation.
- Do not add dependencies outside `fdc-barter`.
- Do not add default network tests.
- Preserve existing `HistoricalBackfillSource` compatibility.
- Keep Barter-rs and exchange REST types inside `fdc-barter`.

## Task 1: Registry and Provider Contracts

Files:

- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- `crates/fdc-adapter/barter/src/ingestion/mod.rs`
- `crates/fdc-adapter/barter/src/lib.rs`
- `crates/fdc-adapter/barter/src/error.rs`
- `crates/fdc-adapter/barter/tests/historical_provider_registry_contract.rs`

Steps:

1. Add `HistoricalProviderCapabilities` with exchange, market types, data kinds, intervals, and max limit.
2. Add `HistoricalExchangeProvider` trait with `capabilities()` and `fetch_page()`.
3. Add `HistoricalProviderRegistry` with registration and routing by normalized exchange.
4. Implement `HistoricalBackfillSource` for registry.
5. Add explicit unsupported exchange/subscription errors.
6. Re-export new types.
7. Add offline tests for dispatch, unsupported exchange, unsupported kind/interval/limit, and source-trait compatibility.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test historical_provider_registry_contract
```

## Task 2: Binance Spot OHLCV REST Descriptor Boundary

Files:

- `crates/fdc-adapter/barter/src/ingestion/historical.rs`
- `crates/fdc-adapter/barter/tests/binance_spot_historical_rest_contract.rs`

Steps:

1. Add `HistoricalRestRequestDescriptor` with method, path, query, timeout.
2. Add `binance_spot_ohlcv_rest_request_descriptor(&HistoricalBackfillRequest)`.
3. Validate that request kind is candle, market type is spot, interval exists, and limit is bounded.
4. Convert nanosecond timestamps to millisecond query params.
5. Add offline contract tests for descriptor shape and rejection of unsupported request kinds.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter --test binance_spot_historical_rest_contract
```

## Task 3: Integration Verification and Status

Files:

- `docs/DEVELOPMENT_STATUS.md`

Steps:

1. Update development status with completed historical REST boundary slice.
2. Run full fdc-barter test suite.
3. Run dependency boundary grep.
4. Commit the implementation.

Verification:

```bash
CARGO_NET_OFFLINE=true rtk cargo fmt --package fdc-barter --check
CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter
grep -R "barter_data\|barter-instrument\|barter_instrument" -n crates \
  | grep -v "crates/fdc-adapter/barter" \
  | grep -v "target" || true
```

## Expected Result

- `fdc-barter` has a multi-exchange historical provider registry.
- First concrete REST boundary targets Binance Spot OHLCV but does not call the network by default.
- Future providers can wrap `barter-integration::protocol::http::RestClient` internally without changing public adapter APIs.
