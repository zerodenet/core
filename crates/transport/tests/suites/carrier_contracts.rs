//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../fingerprint.rs"]
mod fingerprint;
#[path = "../outbound_datagram.rs"]
mod outbound_datagram;
#[path = "../proxy_protocol.rs"]
mod proxy_protocol;
#[path = "../rate_limit.rs"]
mod rate_limit;

#[path = "../channel_stream.rs"]
mod channel_stream;
#[path = "../observed_carrier.rs"]
mod observed_carrier;
