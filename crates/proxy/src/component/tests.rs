mod business_carrier;
mod business_demand;
mod request;

mod service_stream;

mod vmess_ca;

mod direct_stream;

#[cfg(feature = "mieru")]
mod mieru;

#[cfg(all(
    feature = "wireguard",
    feature = "socks5",
    feature = "managed-stream-runtime"
))]
mod wireguard_datagram;

#[cfg(feature = "vless")]
mod vless_grpc_ca;

#[cfg(all(
    feature = "vless",
    feature = "vmess",
    feature = "trojan",
    feature = "shadowsocks",
    feature = "socks5"
))]
mod taskless_node_dns;

#[cfg(any(feature = "vless", feature = "vmess", feature = "trojan"))]
#[path = "tests/mux_datagram.rs"]
mod mux_datagram;

