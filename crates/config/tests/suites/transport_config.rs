//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../browser_dialer.rs"]
mod browser_dialer;
#[path = "../hysteria2_node_transport.rs"]
mod hysteria2_node_transport;
#[path = "../tls_ech.rs"]
mod tls_ech;
#[path = "../tls_options.rs"]
mod tls_options;
#[path = "../trojan_carriers.rs"]
mod trojan_carriers;
#[path = "../vless_carriers.rs"]
mod vless_carriers;
