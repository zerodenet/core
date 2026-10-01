//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../fake_ip_persistence.rs"]
mod fake_ip_persistence;
#[path = "../reverse_mapping.rs"]
mod reverse_mapping;
#[path = "../tun_dns.rs"]
mod tun_dns;
