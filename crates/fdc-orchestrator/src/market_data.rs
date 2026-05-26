use fdc_barter::BarterMarketEvent;
use fdc_core::{error::Error, Result};
use fdc_ingestion::SourceEnvelope;
use fdc_transform::MarketDataDto;

pub fn barter_event_to_market_data_dto(
    _source: &SourceEnvelope<BarterMarketEvent>,
) -> Result<MarketDataDto> {
    Err(Error::unimplemented(
        "barter market data DTO mapping is implemented in Task 3",
    ))
}
