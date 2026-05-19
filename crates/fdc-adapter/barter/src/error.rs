/// Result type used by the Barter adapter.
pub type Result<T> = std::result::Result<T, BarterAdapterError>;

/// Errors produced while adapting Barter data into mdb data.
#[derive(Debug, thiserror::Error)]
pub enum BarterAdapterError {
    /// The Barter event kind is not supported by the current adapter stage.
    #[error("unsupported Barter market data kind: {0}")]
    UnsupportedKind(&'static str),

    /// A Barter numeric value cannot be represented by the target mdb type.
    #[error("invalid numeric value for field {field}: {value}")]
    InvalidNumericValue { field: &'static str, value: f64 },

    /// A timestamp cannot be represented as nanoseconds.
    #[error("timestamp cannot be represented as nanoseconds")]
    InvalidTimestamp,
}
