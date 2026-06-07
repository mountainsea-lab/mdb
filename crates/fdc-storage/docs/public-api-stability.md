# fdc-storage Public API Stability Policy

Date: 2026-06-03
Scope: `crates/fdc-storage` integration-facing API

## Supported public surface

The supported integration-facing API is the set of public items re-exported from `crates/fdc-storage/src/lib.rs`, plus documented module paths used by those re-exports.

Consumers should prefer imports from `fdc_storage::{...}` instead of deep module paths. Deep module paths may remain public for Rust module organization, but re-exports define the compatibility contract.

## Breaking changes

A change is breaking when it removes, renames, or changes the meaning of any re-exported type, trait, enum variant, public field, constructor, or method that integration consumers can call.

Examples of breaking changes:

- Removing a re-export from `lib.rs`.
- Renaming `StorageWriteRecord`, `StorageQuery`, `TieredStorageStore`, or other re-exported types.
- Changing an existing trait method signature.
- Changing lifecycle semantics for TTL hard-delete across tiers.
- Changing serialized field names for persisted or externally exchanged DTOs.

## Additive changes

Additive changes are preferred for P1/P2 hardening:

- Add new methods instead of changing existing method signatures.
- Add new enum variants only when consumers can handle them safely.
- Add serde fields with defaults where practical.
- Keep compatibility methods delegating to newer explicit methods.

## Deprecation policy

Deprecated public APIs should remain available for at least one planned migration window. Immediate removal is reserved for correctness, data-loss, or safety issues and must be documented in the acceptance report.

## Tests

`crates/fdc-storage/tests/public_api_stability.rs` is the smoke test for the re-exported public API. Update it whenever the public API intentionally expands.

## P13 Tiering Policy API Additions

The P13 intelligent tiering policy API is additive. It introduces public policy decision types without removing or changing existing write/query APIs:

- `StorageTieringPolicy`
- `StorageTieringPolicyProfile`
- `StorageTieringContext`
- `StorageTieringDecision`
- `StorageRetentionClass`
- `StorageTieringReason`
- `AccessPatternSnapshot`

Compatibility policy behavior intentionally mirrors the pre-P13 placement routing rules. Business modules should treat `StoragePlacementHint` as advisory and should not depend on hand-selecting physical tiers for ordinary writes. Explicit `target_tier` remains supported for tests, migrations, and operator override scenarios.

## P15 Generic Tag-Aware Policy API Additions

P15 extends the additive policy API with `StorageTieringPolicyProfile::GenericRealtime` and `StorageTieringPolicy::generic_realtime()`. This profile is generic: it reads only `StorageTieringContext` facts and caller-provided metadata tags such as `mode=live`, `mode=backfill`, `quality.is_replay=true`, `data.kind=aggregate`, or `record.kind=candle`.

The profile is not a market-data typed API. Callers remain responsible for mapping any concrete DTO or domain concept into generic storage metadata. `fdc-storage` must not depend on business crates to make these decisions.
