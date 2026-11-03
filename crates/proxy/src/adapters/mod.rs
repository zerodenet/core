//! Concrete protocol capability bridges for each compiled-in protocol.

pub(crate) mod direct;
#[cfg(feature = "http")]
pub(crate) mod http;
#[cfg(feature = "hysteria2")]
pub(crate) mod hysteria2;
mod identity;
#[cfg(feature = "mieru")]
pub(crate) mod mieru;
#[cfg(feature = "mixed")]
pub(crate) mod mixed;
#[cfg(any(feature = "vless", feature = "trojan", feature = "vmess"))]
pub(crate) mod mux_prefix;
#[cfg(feature = "shadowsocks")]
pub(crate) mod shadowsocks;
#[cfg(feature = "socks5")]
pub(crate) mod socks5;
#[cfg(any(feature = "vless", feature = "trojan", feature = "vmess"))]
mod tcp;
#[cfg(feature = "trojan")]
pub(crate) mod trojan;
#[cfg(any(feature = "vless", feature = "vmess", feature = "trojan"))]
mod udp;
#[cfg(feature = "vless")]
pub(crate) mod vless;
#[cfg(feature = "vmess")]
pub(crate) mod vmess;
#[cfg(feature = "wireguard")]
pub(crate) mod wireguard;

pub(crate) mod devices;
