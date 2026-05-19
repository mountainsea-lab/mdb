use barter_instrument::exchange::ExchangeId;

pub fn to_exchange_code(exchange: ExchangeId) -> String {
    exchange.as_str().to_string()
}
