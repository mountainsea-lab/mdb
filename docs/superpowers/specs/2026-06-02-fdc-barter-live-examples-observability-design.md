# fdc-barter Live Examples Observability Design

## Purpose

Manual runs of the fdc-barter live examples should immediately show useful diagnostic output whether launched through `cargo run --example ...` or by clicking the Rust `main` function in an IDE. The output should make it clear which gate or await point is active, what subscriptions are being requested, whether stream initialization completed, how many records were collected, and why a run stopped.

## Scope

In scope:

- Live examples under `crates/fdc-adapter/barter/examples/`:
  - `live_binance_spot_trades.rs`
  - `live_binance_spot_order_books.rs`
  - `live_binance_futures_usd_market_data.rs`
- Use Rust's `tracing` ecosystem for structured diagnostics.
- Keep the public-internet safety gate. If `FDC_BARTER_LIVE_EXAMPLE=1` is missing, examples must not connect to Binance, but they must print a clear runnable command and current gate state.
- Preserve bounded collection and existing 30 second collection timeout.

Out of scope:

- API live runner logging.
- Historical examples, except for future consistency if needed.
- Changing Barter-rs internals.

## Recommended Approach

Use `tracing` in the examples and initialize a local `tracing_subscriber` at example startup. The default filter should be `info`, with `RUST_LOG` support for debug output. This matches the workspace dependency strategy and avoids adding a separate logging stack such as `env_logger`.

`println!` should not be the primary diagnostic path for live examples. User-facing command hints may still be emitted through tracing at `info` level so IDE and terminal launches see them by default.

## Behavior

On every live example start:

1. Initialize tracing subscriber with environment filtering:
   - default filter: `info`
   - respect `RUST_LOG` when set
2. Log the example name and environment gate state.
3. If `FDC_BARTER_LIVE_EXAMPLE != 1`:
   - log that live network access is disabled
   - log the exact `cargo run --example ...` command
   - return `Ok(())`
4. If enabled:
   - log subscription parameters before stream initialization
   - log stream initialization start
   - log stream initialization success
   - log collection start including limit and timeout
   - log each received envelope summary at `info` level for manual diagnosis
   - log final summary with record count, completion flag, reconnect count, and stop reason implied by success or timeout/error

For more verbose investigation, developers can run:

```bash
RUST_LOG=debug FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades
```

## Architecture

Add repeated local helper functions in each live example for this slice. This avoids adding public library API or relying on non-standard shared-example module wiring. If more examples later need the same behavior, extract a dedicated example-support module in a separate cleanup.

Dependencies:

- Add `tracing` and `tracing-subscriber` to `fdc-barter` dev-dependencies for examples only.
- Do not add production adapter tracing events in this change.

## Error Handling

- Missing env gate is informational, not an error.
- Stream initialization errors continue to return `fdc_barter::Result<()>`, but an error log should include context before `?` returns.
- Collection timeout remains represented by `BarterAdapterError::LiveCollectionTimeout` from the existing bounded collection behavior.

## Testing and Verification

Automated tests:

- Add a small testable helper for gate evaluation and command hint formatting if shared logic is introduced.
- Existing live collection timeout test remains the regression coverage for idle stream behavior.

Manual verification:

- `cargo run --example live_binance_spot_trades` without env should print a clear gate-disabled message and runnable command.
- `FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades` should print initialization, subscription, collection, per-record summaries, and final summary.
- `RUST_LOG=debug FDC_BARTER_LIVE_EXAMPLE=1 cargo run --example live_binance_spot_trades` should include debug-level diagnostics if any are added.

## Success Criteria

- A developer clicking `main` in an IDE sees immediate output explaining whether the run is gated or active.
- A developer running cargo examples sees data summaries without modifying code.
- Logging uses `tracing`/`tracing-subscriber`, consistent with workspace dependencies.
- No accidental live network connection occurs unless `FDC_BARTER_LIVE_EXAMPLE=1` is set.
- Existing tests continue to pass.
