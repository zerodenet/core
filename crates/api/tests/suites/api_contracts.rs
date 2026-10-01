//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../api_model.rs"]
mod api_model;
#[path = "../capability_contract.rs"]
mod capability_contract;
#[path = "../endpoints.rs"]
mod endpoints;
#[path = "../forward_compat.rs"]
mod forward_compat;
#[path = "../query_request.rs"]
mod query_request;
#[path = "../response.rs"]
mod response;
#[path = "../traffic.rs"]
mod traffic;
#[path = "../tun_recovery.rs"]
mod tun_recovery;
