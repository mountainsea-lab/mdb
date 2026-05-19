use barter_instrument::instrument::market_data::MarketDataInstrument;
use fdc_core::types::Symbol;

pub fn to_symbol(instrument: &MarketDataInstrument) -> Symbol {
    Symbol::new(format!("{}{}", instrument.base, instrument.quote))
}
