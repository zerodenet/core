//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../quic_http_keepalive.rs"]
mod quic_http_keepalive;
#[path = "../quic_stream.rs"]
mod quic_stream;
#[path = "../quic_trust.rs"]
mod quic_trust;
