use barter_data::{
    event::DataKind,
    streams::{consumer::MarketStreamResult, reconnect},
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures::{Stream, StreamExt};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::BarterIngestionEnvelope,
};

/// Request for bounded live collection from a specific adapter source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveCollectionRequest {
    pub source_id: String,
    pub limit: usize,
}

/// Summary produced by bounded live collection.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveCollectionOutcome {
    pub source_id: String,
    pub envelopes: Vec<BarterIngestionEnvelope>,
    pub records_received: usize,
    pub requested_limit: usize,
    pub complete: bool,
    pub skipped_reconnects: usize,
}

/// Collect live market-data envelopes until the requested limit is reached or the stream ends.
///
/// Reconnect events are skipped and counted in the returned summary.
/// Stream item errors are returned to the caller.
pub async fn collect_live_envelopes_with_summary<S>(
    request: LiveCollectionRequest,
    mut stream: S,
) -> Result<LiveCollectionOutcome>
where
    S: Stream<Item = MarketStreamResult<MarketDataInstrument, DataKind>> + Unpin,
{
    if request.source_id.is_empty() {
        return Err(BarterAdapterError::UnsupportedLiveSubscription(
            "source_id must not be empty".to_string(),
        ));
    }

    if request.limit == 0 {
        return Err(BarterAdapterError::UnsupportedLiveSubscription(
            "limit must be greater than zero".to_string(),
        ));
    }

    let requested_limit = request.limit;
    let source_id = request.source_id;
    let mut envelopes = Vec::with_capacity(requested_limit);
    let mut skipped_reconnects = 0usize;

    while envelopes.len() < requested_limit {
        let Some(result) = stream.next().await else {
            break;
        };

        if matches!(&result, reconnect::Event::Reconnecting(_)) {
            skipped_reconnects += 1;
        }

        if let Some(envelope) = super::live::map_live_market_data_result(&source_id, result)? {
            envelopes.push(envelope);
        }
    }

    let records_received = envelopes.len();

    Ok(LiveCollectionOutcome {
        source_id,
        envelopes,
        records_received,
        requested_limit,
        complete: records_received == requested_limit,
        skipped_reconnects,
    })
}
