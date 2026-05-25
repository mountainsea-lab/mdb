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

    /// Barter-rs failed to initialise a live stream.
    #[error("live stream initialization error: {0}")]
    LiveStreamInit(String),

    /// Barter-rs yielded an error item from a live stream.
    #[error("live stream item error: {0}")]
    LiveStreamItem(String),

    /// A requested live subscription is not supported by this adapter slice.
    #[error("unsupported live subscription: {0}")]
    UnsupportedLiveSubscription(String),

    /// The adapter payload cannot be mapped to a neutral transform DTO.
    #[error("unsupported market data payload for transform DTO mapping: {0}")]
    UnsupportedTransformPayload(String),
}
