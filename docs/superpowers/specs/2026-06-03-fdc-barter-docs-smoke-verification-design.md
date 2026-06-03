# fdc-barter Documentation Calibration and Smoke Verification Design

Date: 2026-06-03
Branch: `mdb-mqdev`

## Goal

Calibrate the `fdc-barter` module documentation to match the current implementation, then run the real-network smoke verification that is normally ignored by default. This slice intentionally does not design or implement the later `fdc-barter` to `fdc-ingestion` glue work.

## Scope

In scope:

1. Update stale `fdc-barter` documentation that still describes the adapter as trade-only or says historical fetchers are not implemented.
2. Preserve accurate remaining gaps, especially production-level order-book gap detection/reconstruction and cross-module integration work.
3. Run ignored `fdc-barter` live and historical smoke tests with the required opt-in environment variables.
4. Record verification results clearly, distinguishing code failures from environment/network/Binance availability failures.

Out of scope:

1. No `fdc-barter` to `fdc-ingestion` bridge design or implementation.
2. No runtime server route changes.
3. No storage, transform, factor engine, or strategy engine work.
4. No changes to unrelated dirty `fdc-server` health files.

## Documentation Design

The primary document to update is:

- `crates/fdc-adapter/barter/docs/market-data-collection-requirements.md`

The update should change stale current-state claims to the implementation state verified on 2026-06-03:

- structured mappings exist for trades, L1 order book, L2 order book, candles, and liquidations;
- Binance Spot historical OHLCV and historical trades REST paths exist;
- bounded live and historical acquisition helpers exist;
- live examples and historical examples exist and compile;
- historical support is currently Binance Spot focused, not multi-exchange complete.

If useful, `docs/DEVELOPMENT_STATUS.md` may be updated only to add today's verification checkpoint. It should not be rewritten broadly.

## Smoke Verification Design

Run the ignored smoke tests explicitly:

- historical smoke with `FDC_BARTER_HISTORICAL_SMOKE=1`;
- live smoke with `FDC_BARTER_LIVE_SMOKE=1`.

The smoke tests require public internet and Binance access. If a test fails due to DNS, connection timeout, rate limit, regional block, or Binance service behavior, record that as an environment/network result and do not treat it as proof that the offline contract implementation is broken.

Offline validation remains required before completion:

- `CARGO_NET_OFFLINE=true rtk cargo test -p fdc-barter`;
- `CARGO_NET_OFFLINE=true rtk cargo check -p fdc-barter --example <each fdc-barter example>`.

## Success Criteria

1. Documentation no longer contradicts current `fdc-barter` implementation.
2. Remaining gaps are explicit and scoped.
3. Offline `fdc-barter` tests still pass.
4. All five examples still compile.
5. Ignored smoke tests are attempted and results are recorded in the final report.
6. No unrelated `fdc-server` health changes are committed.
