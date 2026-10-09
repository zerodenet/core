//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../egress_interface.rs"]
mod egress_interface;
#[path = "../listener_latency.rs"]
mod listener_latency;
#[path = "../process_lookup.rs"]
mod process_lookup;
#[path = "../tcp_nodelay.rs"]
mod tcp_nodelay;

#[path = "../dial_policy.rs"]
mod dial_policy;
