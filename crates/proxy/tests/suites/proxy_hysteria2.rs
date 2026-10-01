//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/mod.rs"]
mod support;

#[path = "../hysteria2_shared_connection.rs"]
mod hysteria2_shared_connection;
#[path = "../hysteria2_tls_policy.rs"]
mod hysteria2_tls_policy;
#[path = "../hysteria2_website.rs"]
mod hysteria2_website;
