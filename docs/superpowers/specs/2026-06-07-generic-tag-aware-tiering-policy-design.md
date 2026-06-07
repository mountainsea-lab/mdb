# P15 Generic Tag-Aware Tiering Policy Design

Date: 2026-06-07
Module: `crates/fdc-storage` policy, with `fdc-server` runtime profile selection
Status: approved design for implementation

## Goal

Add a generic, tag-aware storage tiering policy profile that makes better automatic initial placement decisions while keeping `fdc-storage` a business-agnostic storage layer.

## Boundary rule

`fdc-storage` must remain generic. It must not depend on market-data DTOs or on barter, ingestion, transform, orchestrator, server, or API crates. The caller is responsible for mapping domain-specific data into generic storage records and generic metadata tags.

The policy may inspect only `StorageTieringContext` fields:

- namespace and collection strings
- key/value sizes
- timestamp age
- generic metadata tags
- advisory placement hints
- available tiers
- prior generic access snapshot

## Profile name

Add `StorageTieringPolicyProfile::GenericRealtime`.

The name is intentionally not market-data-specific. It represents a generic low-latency / recent-data profile driven by tags and storage facts.

## Generic tag vocabulary

The profile recognizes these optional string tags:

- `mode=live`
- `mode=backfill`
- `quality.is_replay=true`
- `data.kind=aggregate`
- `data.kind=candle`
- `record.kind=aggregate`
- `record.kind=candle`

These names are generic and advisory. Unknown tags are ignored. Callers may use any data type as long as they encode storage-relevant facts in tags.

## Decision rules

Explicit target tier hints remain operator overrides and win first.

After explicit target hints, the profile applies generic facts:

1. Replay or backfill records prefer L3 and `AnalyticalWarm`.
2. Aggregate/candle-like records prefer L3 and `AnalyticalWarm`.
3. Old records prefer L3 or L4 depending on age:
   - older than 7 days: L3
   - older than 90 days: L4 and `ArchiveCold`
4. Large payloads avoid L1. Payloads over 1 MiB prefer L3.
5. Live recent records prefer L2 and `RealtimeHot`. If durability is `Ephemeral`, they may use L1.
6. If no tag/fact rule applies, fallback to compatibility behavior.

All desired tiers are passed through nearest-available-tier fallback.

## Explainability

Add generic reasons to `StorageTieringReason`:

- `LiveRecentWrite`
- `BackfillOrReplay`
- `AggregateRecord`
- `LargePayload`
- `OldTimestamp`

These reasons do not encode business-specific DTO names.

## Server runtime profile

Add `MarketDataStoragePolicyProfileConfig::GenericRealtime` in `fdc-server` so runtime config can accept:

```text
FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime
```

P15 does not need full policy injection into `TieredStorageStore` yet because P13 routing still uses compatibility internally. Server acceptance is a runtime contract step. A later slice can thread selected policy profiles into the tiered store once the store owns a configurable policy field.

## Testing

Add storage policy tests proving:

- `GenericRealtime` profile exists and is public.
- explicit target tier still wins.
- `mode=live` fresh records choose a hot tier.
- `mode=backfill` and `quality.is_replay=true` choose analytical/cold tiers.
- aggregate/candle tags choose analytical warm placement.
- large payloads avoid L1.
- old timestamps choose colder tiers.
- unknown tags fall back to compatibility behavior.

Add server runtime tests proving:

- `FDC_MARKET_DATA_STORAGE_POLICY_PROFILE=generic_realtime` parses successfully.
- unsupported profile values still fail clearly.

## Out of scope

- Adding market-data DTO dependencies to `fdc-storage`.
- Defining a typed market-data policy inside `fdc-storage`.
- Requiring callers to manually set physical tiers per write.
- Full runtime policy injection into `TieredStorageStore`.
- Scheduled maintenance or adaptive promotion/demotion.

## Self-review

- Placeholder scan: no placeholder requirements remain.
- Boundary check: storage remains generic and only reads generic tags/facts.
- Scope check: one implementation slice, centered on policy profile and runtime profile parsing.
- Ambiguity check: `generic_realtime` is accepted by server config but full tiered-store policy injection is explicitly deferred.
