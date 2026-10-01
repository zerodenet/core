//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/mod.rs"]
mod support;

#[path = "../build_info.rs"]
mod build_info;
#[path = "../config_errors.rs"]
mod config_errors;
#[path = "../connector_state.rs"]
mod connector_state;
#[path = "../live_status.rs"]
mod live_status;
#[path = "../management_lifecycle.rs"]
mod management_lifecycle;
#[path = "../parent_lifetime.rs"]
mod parent_lifetime;
#[path = "../phase1_port_conflict.rs"]
mod phase1_port_conflict;
#[path = "../proxy_smoke.rs"]
mod proxy_smoke;
#[path = "../status.rs"]
mod status;
