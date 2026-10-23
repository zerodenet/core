#![cfg(all(
    feature = "socks5",
    any(feature = "trojan", feature = "vmess", feature = "vless")
))]
mod fixtures;
#[cfg(feature = "trojan")]
mod trojan;
#[cfg(feature = "vmess")]
mod vmess;

#[cfg(feature = "vless")]
mod vless;
