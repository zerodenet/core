//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/host.rs"]
mod host;
#[path = "../support/mod.rs"]
mod support;

#[path = "../packet_translation_boundary.rs"]
mod packet_translation_boundary;
#[path = "../wireguard_echo_translation.rs"]
mod wireguard_echo_translation;
#[path = "../wireguard_health.rs"]
mod wireguard_health;
#[path = "../wireguard_local_delivery.rs"]
mod wireguard_local_delivery;
#[path = "../wireguard_network_recovery.rs"]
mod wireguard_network_recovery;
#[path = "../wireguard_packet_non_echo.rs"]
mod wireguard_packet_non_echo;
#[path = "../wireguard_target_dns.rs"]
mod wireguard_target_dns;
#[path = "../wireguard_traffic.rs"]
mod wireguard_traffic;
