//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../endpoint_control.rs"]
mod endpoint_control;
#[path = "../endpoint_metadata.rs"]
mod endpoint_metadata;
#[path = "../endpoints.rs"]
mod endpoints;

#[path = "../packet_routes.rs"]
mod packet_routes;
