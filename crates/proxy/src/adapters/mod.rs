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
#[cfg(feature = "shadowsocks")]
pub(crate) mod shadowsocks;
#[cfg(feature = "socks5")]
pub(crate) mod socks5;
#[cfg(feature = "trojan")]
pub(crate) mod trojan;
#[cfg(feature = "vless")]
pub(crate) mod vless;
#[cfg(feature = "vmess")]
pub(crate) mod vmess;
#[cfg(feature = "wireguard")]
pub(crate) mod wireguard;
