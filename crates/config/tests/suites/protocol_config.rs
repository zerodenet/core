//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../hysteria2.rs"]
mod hysteria2;
#[path = "../hysteria2_website.rs"]
mod hysteria2_website;
#[path = "../mieru_transport.rs"]
mod mieru_transport;
#[path = "../reality_config.rs"]
mod reality_config;
#[path = "../shadowsocks_options.rs"]
mod shadowsocks_options;
#[path = "../vless_encryption.rs"]
mod vless_encryption;
#[path = "../vless_identity.rs"]
mod vless_identity;
#[path = "../vless_reverse.rs"]
mod vless_reverse;
#[path = "../vless_test_parameters.rs"]
mod vless_test_parameters;
#[path = "../vmess_cipher_names.rs"]
mod vmess_cipher_names;
#[path = "../wireguard.rs"]
mod wireguard;
