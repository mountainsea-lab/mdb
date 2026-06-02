use async_trait::async_trait;
use barter_data::{
    event::DataKind,
    streams::{consumer::MarketStreamResult, reconnect},
};
use barter_instrument::instrument::market_data::MarketDataInstrument;
use futures::{Stream, StreamExt};

use crate::{
    error::{BarterAdapterError, Result},
    ingestion::{
        execute_binance_spot_historical_trades_rest, execute_binance_spot_ohlcv_rest,
        live::map_live_market_data_result, BarterIngestionEnvelope, HistoricalBackfillPage,
        HistoricalBackfillRequest, HistoricalRestExecutor,
    },
    model::HistoricalCursor,
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

/// Stop condition emitted by the bounded historical backfill runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalBackfillStopReason {
    SourceComplete,
    MaxPagesReached,
    MaxRecordsReached,
}

/// Request for bounded historical backfill pagination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalBackfillRunRequest {
    pub first_request: HistoricalBackfillRequest,
    pub max_pages: usize,
    pub max_records: Option<usize>,
}

/// Summary of a bounded historical backfill run.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoricalBackfillRunOutcome {
    pub pages: Vec<HistoricalBackfillPage>,
    pub records_received: usize,
    pub final_cursor: Option<HistoricalCursor>,
    pub complete: bool,
    pub stopped_reason: HistoricalBackfillStopReason,
}

#[async_trait]
pub trait HistoricalPageFetcher: Send + Sync {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage>;
}

pub async fn run_historical_backfill_pages<F>(
    fetcher: &F,
    request: HistoricalBackfillRunRequest,
) -> Result<HistoricalBackfillRunOutcome>
where
    F: HistoricalPageFetcher + ?Sized,
{
    if request.max_pages == 0 {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "historical backfill max_pages must be greater than zero".to_string(),
        ));
    }

    if request.max_records == Some(0) {
        return Err(BarterAdapterError::InvalidHistoricalRequest(
            "historical backfill max_records must be greater than zero when provided".to_string(),
        ));
    }

    let HistoricalBackfillRunRequest {
        first_request,
        max_pages,
        max_records,
    } = request;

    let mut next_request = first_request;
    let mut pages = Vec::with_capacity(max_pages);
    let mut records_received = 0usize;

    loop {
        let page = fetcher.fetch_page(next_request.clone()).await?;
        records_received += page.envelopes.len();
        let final_cursor = page.next_cursor.clone();
        let source_complete = page.complete;

        pages.push(page);

        if source_complete {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: true,
                stopped_reason: HistoricalBackfillStopReason::SourceComplete,
            });
        }

        if max_records.is_some_and(|max_records| records_received >= max_records) {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: false,
                stopped_reason: HistoricalBackfillStopReason::MaxRecordsReached,
            });
        }

        if pages.len() >= max_pages {
            return Ok(HistoricalBackfillRunOutcome {
                pages,
                records_received,
                final_cursor,
                complete: false,
                stopped_reason: HistoricalBackfillStopReason::MaxPagesReached,
            });
        }

        let cursor = final_cursor.clone().ok_or_else(|| {
            BarterAdapterError::InvalidHistoricalRequest(
                "historical backfill page missing next_cursor before completion".to_string(),
            )
        })?;
        let next_start = cursor.next_start.ok_or_else(|| {
            BarterAdapterError::InvalidHistoricalRequest(
                "historical backfill page missing next_cursor.next_start before completion"
                    .to_string(),
            )
        })?;

        next_request.start = next_start;
        next_request.cursor = Some(cursor);
    }
}

#[derive(Clone, Copy)]
pub struct BinanceSpotOhlcvHistoricalPageFetcher<'a> {
    executor: &'a dyn HistoricalRestExecutor,
}

impl<'a> BinanceSpotOhlcvHistoricalPageFetcher<'a> {
    pub fn new(executor: &'a dyn HistoricalRestExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl HistoricalPageFetcher for BinanceSpotOhlcvHistoricalPageFetcher<'_> {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        execute_binance_spot_ohlcv_rest(self.executor, request).await
    }
}

#[derive(Clone, Copy)]
pub struct BinanceSpotTradesHistoricalPageFetcher<'a> {
    executor: &'a dyn HistoricalRestExecutor,
}

impl<'a> BinanceSpotTradesHistoricalPageFetcher<'a> {
    pub fn new(executor: &'a dyn HistoricalRestExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl HistoricalPageFetcher for BinanceSpotTradesHistoricalPageFetcher<'_> {
    async fn fetch_page(
        &self,
        request: HistoricalBackfillRequest,
    ) -> Result<HistoricalBackfillPage> {
        execute_binance_spot_historical_trades_rest(self.executor, request).await
    }
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
        return Err(BarterAdapterError::InvalidLiveCollectionRequest(
            "source_id must not be empty".to_string(),
        ));
    }

    if request.limit == 0 {
        return Err(BarterAdapterError::InvalidLiveCollectionRequest(
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

        if let Some(envelope) = map_live_market_data_result(&source_id, result)? {
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
