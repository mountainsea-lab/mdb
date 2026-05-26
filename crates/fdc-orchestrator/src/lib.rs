//! Cross-layer orchestration glue for Financial Data Center.
//!
//! This crate is the approved home for concrete adapter -> ingestion -> transform
//! -> storage mapping. Core crates must not depend on this crate.
//!
//! B6 implements market-data glue for the Barter adapter first. The crate boundary
//! stays adapter-extensible so later slices can add other source modules without
//! moving cross-layer mapping into core crates.

pub mod barter;
pub mod market_data;
pub mod pipeline;
pub mod storage;
