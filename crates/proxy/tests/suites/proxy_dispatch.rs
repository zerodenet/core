//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/mod.rs"]
mod support;

#[path = "../direct.rs"]
mod direct;
#[path = "../direct_datagram.rs"]
mod direct_datagram;
#[path = "../direct_udp_policy.rs"]
mod direct_udp_policy;
#[path = "../http.rs"]
mod http;
#[path = "../mixed.rs"]
mod mixed;
#[path = "../socks5.rs"]
mod socks5;
#[path = "../socks5_udp.rs"]
mod socks5_udp;
#[path = "../socks5_udp_idle.rs"]
mod socks5_udp_idle;
#[path = "../socks5_udp_reuse.rs"]
mod socks5_udp_reuse;
#[path = "../trojan.rs"]
mod trojan;
#[path = "../vmess.rs"]
mod vmess;
