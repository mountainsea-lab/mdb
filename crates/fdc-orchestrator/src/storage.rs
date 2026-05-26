use fdc_core::{error::Error, Result};
use fdc_storage::StorageWriteRecord;
use fdc_transform::MarketDataDto;

pub fn market_data_dto_to_storage_record(_dto: &MarketDataDto) -> Result<StorageWriteRecord> {
    Err(Error::unimplemented(
        "market data DTO storage mapping is implemented in Task 4",
    ))
}
