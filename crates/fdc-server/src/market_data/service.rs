use std::{sync::Arc, time::Duration};

use fdc_barter::{
    collect_live_trade_envelopes, default_binance_spot_trade_subscriptions,
    init_binance_spot_public_trades, public_trade_result_to_data_kind, BarterIngestionEnvelope,
    BarterMarketDataKind, BarterMarketDataMode, BarterMarketEvent, BarterMarketPayload,
    DataQualityFlags, TradePayload, TradeSide,
};
use fdc_core::{
    types::{Price, Symbol, TimestampNs},
    Result,
};
use fdc_storage::{MarketDataQuery, QueryableMarketDataStore, StorageWriteRecord};
use futures::{stream, StreamExt};
use rust_decimal::Decimal;

use crate::{
    market_data::model::{
        MarketDataLiveState, MarketDataTradeRecord, MarketDataTradesResponse,
        StartLiveMarketDataRequest, StartLiveMarketDataResponse, StopLiveMarketDataResponse,
    },
    run_realtime_barter_envelope_stream, ProductionServerState, RealtimeMarketDataMvpConfig,
};

pub fn live_status(
    state: &ProductionServerState,
) -> crate::market_data::model::LiveMarketDataStatusResponse {
    state.market_data_supervisor().status()
}

pub fn start_live_disabled(state: &ProductionServerState) -> (StartLiveMarketDataResponse, String) {
    let status = state.market_data_supervisor().status();
    (
        StartLiveMarketDataResponse {
            state: status.state,
            task_id: status.task_id,
            envelopes_received: 0,
            storage_records_written: 0,
            market_data_store_records: state.market_data_store().record_count(),
        },
        "live market-data acquisition requires FDC_LIVE_ENABLED=1".to_string(),
    )
}

pub async fn start_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    start_background_live(state, request).await
}

pub async fn start_background_live(
    state: &ProductionServerState,
    request: StartLiveMarketDataRequest,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    if !state.config().live_enabled {
        let (_, message) = start_live_disabled(state);
        return Err(message);
    }

    let subscriptions = vec![
        "binance_spot:BTCUSDT:public_trades".to_string(),
        "binance_spot:ETHUSDT:public_trades".to_string(),
    ];
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(subscriptions)
        .map_err(|error| error.to_string())?;
    let timeout_secs = request
        .timeout_secs
        .unwrap_or(state.config().live_default_timeout_secs)
        .max(1);
    let max_envelopes = request
        .max_envelopes
        .unwrap_or(state.config().live_default_max_envelopes)
        .max(1);
    let store = state.market_data_store();
    let supervisor_for_task = state.market_data_supervisor();

    tokio::spawn(async move {
        if let Err(message) = run_background_live_collection(
            supervisor_for_task.clone(),
            store,
            timeout_secs,
            max_envelopes,
        )
        .await
        {
            supervisor_for_task.fail(message);
        }
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}

pub fn stop_live(state: &ProductionServerState) -> StopLiveMarketDataResponse {
    let supervisor = state.market_data_supervisor();
    supervisor.request_stop("requested");
    let status = supervisor.status();
    StopLiveMarketDataResponse {
        state: status.state,
        task_id: status.task_id,
        stopped_at_ns: status.stopped_at_ns,
        stop_reason: status.stop_reason,
    }
}

pub async fn start_fake_background_live_for_test(
    state: &ProductionServerState,
    ticks: usize,
    interval: Duration,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    let supervisor = state.market_data_supervisor();
    let task_id = supervisor
        .start_background(vec!["test:fake:trades".to_string()])
        .map_err(|error| error.to_string())?;
    let supervisor_for_task = state.market_data_supervisor();
    let store = state.market_data_store();
    tokio::spawn(async move {
        for index in 0..ticks {
            if supervisor_for_task.stop_requested() {
                supervisor_for_task.stopped("requested");
                return;
            }
            tokio::time::sleep(interval).await;
            supervisor_for_task.record_progress(
                1,
                1,
                store.record_count() + index + 1,
                Some(TimestampNs::now().as_nanos().max(0) as u64),
            );
        }
        while !supervisor_for_task.stop_requested() {
            tokio::time::sleep(interval).await;
        }
        supervisor_for_task.stopped("requested");
    });

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Running,
        task_id: Some(task_id),
        envelopes_received: 0,
        storage_records_written: 0,
        market_data_store_records: state.market_data_store().record_count(),
    })
}

pub fn query_trades(
    state: &ProductionServerState,
    symbol: Option<String>,
    limit: Option<usize>,
) -> MarketDataTradesResponse {
    let mut query = MarketDataQuery::for_trades();
    if let Some(symbol) = symbol {
        query = query.with_symbol(symbol);
    }
    if let Some(limit) = limit {
        query = query.with_limit(limit);
    }

    let records: Vec<_> = state
        .market_data_store()
        .query(&query)
        .into_iter()
        .map(record_to_trade_record)
        .collect();

    MarketDataTradesResponse {
        returned_records: records.len(),
        records,
    }
}

pub async fn ingest_test_trade(
    state: &ProductionServerState,
    symbol: &str,
    trade_id: &str,
) -> Result<StartLiveMarketDataResponse> {
    let envelope = test_trade_envelope(symbol, trade_id);
    let summary = run_realtime_barter_envelope_stream(
        futures::stream::iter(vec![envelope]),
        state.market_data_store(),
        RealtimeMarketDataMvpConfig::default(),
    )
    .await?;

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        task_id: None,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}

async fn run_background_live_collection(
    supervisor: Arc<crate::market_data::supervisor::MarketDataSupervisor>,
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<(), String> {
    while !supervisor.stop_requested() {
        let before = store.record_count();
        let result =
            run_live_collection_and_storage(store.clone(), timeout_secs, max_envelopes).await?;
        let after = store.record_count();
        supervisor.record_progress(
            result.envelopes_received,
            result.storage_records_written,
            after,
            Some(TimestampNs::now().as_nanos().max(0) as u64),
        );
        if after == before && supervisor.stop_requested() {
            break;
        }
    }
    supervisor.stopped("requested");
    Ok(())
}

async fn run_live_collection_and_storage(
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to create production live runner runtime: {error}"))?;

        runtime.block_on(run_live_collection_and_storage_on_current_thread(
            store,
            timeout_secs,
            max_envelopes,
        ))
    })
    .await
    .map_err(|error| format!("production live runner task failed to join: {error}"))?
}

async fn run_live_collection_and_storage_on_current_thread(
    store: Arc<QueryableMarketDataStore>,
    timeout_secs: u64,
    max_envelopes: usize,
) -> std::result::Result<StartLiveMarketDataResponse, String> {
    eprintln!(
        "fdc production live runner: starting Binance Spot public trades timeout_secs={timeout_secs} max_envelopes={max_envelopes}"
    );

    let streams = init_binance_spot_public_trades(default_binance_spot_trade_subscriptions())
        .await
        .map_err(|error| {
            eprintln!("fdc production live runner: failed to initialize stream: {error}");
            format!("failed to initialize Binance Spot live stream: {error}")
        })?;

    let stream = streams.select_all().map(public_trade_result_to_data_kind);
    let envelopes = match tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        collect_live_trade_envelopes("barter-binance-spot-live-trades", stream, max_envelopes),
    )
    .await
    {
        Ok(Ok(envelopes)) => envelopes,
        Ok(Err(error)) => {
            eprintln!("fdc production live runner: stream item error: {error}");
            return Err(format!("failed while collecting live trades: {error}"));
        }
        Err(_) => {
            eprintln!("fdc production live runner: timed out while collecting live trades");
            return Err(format!(
                "timed out after {timeout_secs}s while collecting live trades"
            ));
        }
    };

    eprintln!(
        "fdc production live runner: collected {} live envelopes, writing to queryable store",
        envelopes.len()
    );

    if envelopes.is_empty() {
        return Err("live stream returned no trade envelopes".to_string());
    }

    let summary = run_realtime_barter_envelope_stream(
        stream::iter(envelopes),
        store,
        RealtimeMarketDataMvpConfig {
            runtime_window: Duration::from_secs(timeout_secs.max(1)),
            idle_timeout: Duration::from_millis(100),
            max_errors: 0,
        },
    )
    .await
    .map_err(|error| {
        eprintln!("fdc production live runner: failed to write live envelopes: {error}");
        format!("failed to write live envelopes: {error}")
    })?;

    eprintln!(
        "fdc production live runner: completed envelopes_received={} storage_records_written={} market_data_store_records={}",
        summary.envelopes_received,
        summary.storage_records_written,
        summary.market_data_store_records
    );

    Ok(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        task_id: None,
        envelopes_received: summary.envelopes_received,
        storage_records_written: summary.storage_records_written,
        market_data_store_records: summary.market_data_store_records,
    })
}

fn record_to_trade_record(record: StorageWriteRecord) -> MarketDataTradeRecord {
    let payload = serde_json::from_slice(&record.value).unwrap_or_else(|_| serde_json::Value::Null);
    MarketDataTradeRecord {
        key: String::from_utf8_lossy(&record.key).to_string(),
        symbol: record.metadata.tags.get("symbol").cloned(),
        kind: record.metadata.tags.get("kind").cloned(),
        source: record.metadata.source.clone(),
        payload,
    }
}

fn test_trade_envelope(symbol: &str, trade_id: &str) -> BarterIngestionEnvelope {
    let event = BarterMarketEvent {
        source: "barter".to_string(),
        mode: BarterMarketDataMode::Live,
        exchange: "binance_spot".to_string(),
        symbol: Symbol::new(symbol),
        kind: BarterMarketDataKind::Trade,
        timestamp: TimestampNs::now(),
        received_at: TimestampNs::now(),
        payload: BarterMarketPayload::Trade(TradePayload {
            trade_id: Some(trade_id.to_string()),
            price: Price::new(Decimal::new(42_000_00, 2)),
            quantity: Decimal::new(1, 0),
            side: Some(TradeSide::Buy),
        }),
        sequence: Some(format!("seq-{trade_id}")),
        checkpoint: None,
    };

    let mut envelope = BarterIngestionEnvelope::from_event("barter:binance_spot", event);
    envelope.envelope_id = format!("env-{trade_id}");
    envelope.quality = DataQualityFlags::default();
    envelope
}
