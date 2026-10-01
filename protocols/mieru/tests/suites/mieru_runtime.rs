//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../inbound_route.rs"]
mod inbound_route;
#[path = "../multiplex.rs"]
mod multiplex;
#[path = "../traffic_stream.rs"]
mod traffic_stream;
#[path = "../underlay.rs"]
mod underlay;
