# fdc-barter Module Docs

This directory contains module-level design notes for `crates/fdc-adapter/barter`.

Documents:

- [Market Data Collection Requirements Design](./market-data-collection-requirements.md)
- [Market Data Collection Implementation Plan](./market-data-collection-implementation-plan.md)

Guideline:

- Keep Barter-specific acquisition, mapping, capability, and historical-data designs here.
- Keep cross-module platform architecture under repository-level `docs/`.
- Preserve crate boundaries: this module may depend on Barter-rs, but generic ingestion, transform, storage, and server layers should not embed Barter-rs details.
