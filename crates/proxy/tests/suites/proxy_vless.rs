//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/mod.rs"]
mod support;

#[path = "../vless.rs"]
mod vless;
#[path = "../vless_associated_datagram_relay.rs"]
mod vless_associated_datagram_relay;
#[path = "../vless_datagram_relay.rs"]
mod vless_datagram_relay;
#[path = "../vless_ech_h3.rs"]
mod vless_ech_h3;
#[path = "../vless_ech_legacy.rs"]
mod vless_ech_legacy;
#[path = "../vless_finalmask.rs"]
mod vless_finalmask;
#[path = "../vless_grpc_options.rs"]
mod vless_grpc_options;
#[path = "../vless_hysteria.rs"]
mod vless_hysteria;
#[path = "../vless_mkcp.rs"]
mod vless_mkcp;
#[path = "../vless_proxy_preamble.rs"]
mod vless_proxy_preamble;
#[path = "../vless_quic_ca.rs"]
mod vless_quic_ca;
#[path = "../vless_reality_extensions.rs"]
mod vless_reality_extensions;
#[path = "../vless_reverse.rs"]
mod vless_reverse;
#[path = "../vless_vision_udp.rs"]
mod vless_vision_udp;
#[path = "../vless_xhttp_relay.rs"]
mod vless_xhttp_relay;
