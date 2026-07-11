# Binance Futures USD Derivatives Historical Validation Report

**Date:** 2026-07-11  
**Module:** `crates/fdc-adapter/barter`  
**Stage:** Stage 1, Binance Futures USD / Perpetual adapter validation  
**Status:** `validated`

## Scope validated

This report validates adapter-owned historical acquisition for Binance Futures USD perpetual public data:

| Data | Endpoint | Adapter kind | Payload |
|---|---|---|---|
| Funding rate history | `/fapi/v1/fundingRate` | `FundingRate` | `FundingRatePayload` |
| Open interest snapshot | `/fapi/v1/openInterest` | `OpenInterest` | `OpenInterestPayload` |
| Premium index / mark price | `/fapi/v1/premiumIndex` | `MarkPrice` | `MarkPricePayload` |
| Futures OHLCV kline | `/fapi/v1/klines` | `Candle` | `CandlePayload` |

The stage remains adapter-only. It does not write to `fdc-storage`, expose server query APIs, or compute factors.

## Delivered code

- Derivatives market-data model extensions:
  - `BarterMarketDataKind::{FundingRate, OpenInterest, MarkPrice, IndexPrice}`
  - `BarterMarketPayload::{FundingRate, OpenInterest, MarkPrice, IndexPrice}`
  - Payloads: `FundingRatePayload`, `OpenInterestPayload`, `MarkPricePayload`, `IndexPricePayload`
- Binance Futures USD historical REST descriptors and capabilities.
- Offline fixture providers for funding rate, open interest, premium index / mark price, and futures OHLCV.
- REST execution helpers via `HistoricalRestExecutor`:
  - `execute_binance_futures_usd_funding_rate_rest`
  - `execute_binance_futures_usd_open_interest_rest`
  - `execute_binance_futures_usd_mark_price_rest`
  - `execute_binance_futures_usd_ohlcv_rest`
- Real REST executor constructor:
  - `BarterIntegrationHistoricalRestExecutor::binance_futures_usd()`
- Runnable example:
  - `crates/fdc-adapter/barter/examples/historical_binance_futures_usd_derivatives.rs`
  - Default mode calls real Binance Futures REST and prints visible records.
  - Offline fixture mode is opt-in with `MDB_BARTER_EXAMPLE_MODE=fixture`.

## Validation commands

```bash
rtk cargo test -p fdc-barter derivatives_payloads_report_expected_kinds
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_contract
rtk cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
rtk cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract
rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
MDB_BARTER_EXAMPLE_MODE=fixture rtk cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
rtk cargo test -p fdc-barter
```

## Validation results

Latest verification results from this stage:

```text
cargo test -p fdc-barter --test binance_futures_derivatives_provider_contract
  5 passed

cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
  mode=real_network
  fetched public Binance Futures REST data from /fapi/v1/fundingRate, /fapi/v1/openInterest, /fapi/v1/premiumIndex, and /fapi/v1/klines

MDB_BARTER_EXAMPLE_MODE=fixture cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
  mode=fixture
  emitted visible endpoint/exchange/market_type/symbol/kind/records/first_event_time/first_payload/complete lines without network access

cargo test -p fdc-barter --test binance_futures_historical_rest_execution_contract
  4 passed, 1 ignored

cargo test -p fdc-barter
  77 passed, 7 ignored
```

## Example output evidence

Default example mode calls real Binance Futures REST and produces visible records:

```text
example=historical_binance_futures_usd_derivatives mode=real_network symbol=BTCUSDT
endpoint=/fapi/v1/fundingRate exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=FundingRate records=1 first_event_time=1783756800000000000 first_payload=Some(FundingRate(FundingRatePayload { funding_rate: 0.00000942, funding_time: TimestampNs(1783756800000000000), mark_price: Some(Price(64132.60000000)) })) complete=false
endpoint=/fapi/v1/openInterest exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=OpenInterest records=1 first_event_time=1783785675026468000 first_payload=Some(OpenInterest(OpenInterestPayload { open_interest: 102389.451, timestamp: TimestampNs(1783785675026468000) })) complete=true
endpoint=/fapi/v1/premiumIndex exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=MarkPrice records=1 first_event_time=1783785675004000000 first_payload=Some(MarkPrice(MarkPricePayload { mark_price: Price(64123.50000000), index_price: Some(Price(64159.38282609)), estimated_settle_price: Some(Price(64182.98033394)), funding_rate: Some(0.00004156), next_funding_time: Some(TimestampNs(1783814400000000000)) })) complete=true
endpoint=/fapi/v1/klines exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=Candle records=1 first_event_time=1783785120000000000 first_payload=Some(Candle(CandlePayload { interval: Some("1m"), open_time: TimestampNs(1783785120000000000), close_time: TimestampNs(1783785179999000000), open: Price(64017.20), high: Price(64076.80), low: Price(64017.20), close: Price(64053.80), volume: 108.578, trade_count: Some(2453), quote_volume: Some(6955110.03820) })) complete=false
example=historical_binance_futures_usd_derivatives complete=true
```

Offline fixture mode is still available for deterministic, no-network troubleshooting:

```text
MDB_BARTER_EXAMPLE_MODE=fixture cargo run -p fdc-barter --example historical_binance_futures_usd_derivatives
example=historical_binance_futures_usd_derivatives mode=fixture symbol=BTCUSDT
endpoint=/fapi/v1/fundingRate exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=FundingRate records=1 first_payload=Some(FundingRate(...)) complete=false
endpoint=/fapi/v1/openInterest exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=OpenInterest records=1 first_payload=Some(OpenInterest(...)) complete=true
endpoint=/fapi/v1/premiumIndex exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=MarkPrice records=1 first_payload=Some(MarkPrice(...)) complete=true
endpoint=/fapi/v1/klines exchange=binance_futures_usd market_type=Perpetual symbol=BTCUSDT kind=Candle records=1 first_payload=Some(Candle(...)) complete=false
example=historical_binance_futures_usd_derivatives complete=true
```

## Downstream adapter envelope contract

Downstream modules should consume the normalized adapter envelope, not Binance raw response schemas.

```text
source_id=example-binance-futures-usd-derivatives | barter-binance-futures-usd-history
exchange=binance_futures_usd
market_type=Perpetual
symbol=BTCUSDT
mode=Historical
kind=FundingRate | OpenInterest | MarkPrice | Candle
quality.is_backfill=true
timestamp=<exchange event time when available, adapter receive time for current snapshots>
received_at=<adapter receive time>
payload=<typed BarterMarketPayload variant>
```

Payload field contract:

```text
FundingRatePayload:
  funding_rate: Decimal
  funding_time: TimestampNs
  mark_price: Option<Price>

OpenInterestPayload:
  open_interest: Decimal
  timestamp: TimestampNs

MarkPricePayload:
  mark_price: Price
  index_price: Option<Price>
  estimated_settle_price: Option<Price>
  funding_rate: Option<Decimal>
  next_funding_time: Option<TimestampNs>

CandlePayload:
  interval: Option<String>
  open_time: TimestampNs
  close_time: TimestampNs
  open/high/low/close: Price
  volume: Decimal
  trade_count: Option<u64>
  quote_volume: Option<Decimal>
```

## Explicitly unsupported in Stage 1

- No storage writes or queries.
- No `fdc-server` HTTP query API changes.
- No factor calculation.
- No non-Binance exchanges.
- No historical open-interest time series endpoint `/futures/data/openInterestHist`.
- No futures WebSocket metrics beyond existing live market-data coverage.
- `IndexPricePayload` is modeled for future exchange-specific endpoints, but Binance premium index currently maps to `MarkPricePayload` with optional `index_price`.

## Next stage handoff

Stage 2 can start from the adapter envelope contract above and map `BarterIngestionEnvelope` records into storage write inputs. Stage 2 should preserve:

- `exchange`
- `market_type`
- `symbol`
- `kind`
- `mode`
- event `timestamp`
- `received_at`
- typed payload fields
- `quality.is_backfill`
