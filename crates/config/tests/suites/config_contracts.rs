//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../auth.rs"]
mod auth;
#[path = "../config_parse.rs"]
mod config_parse;
#[path = "../connector_webhook.rs"]
mod connector_webhook;
#[path = "../endpoints.rs"]
mod endpoints;
#[path = "../schema_version.rs"]
mod schema_version;

#[path = "../listener_addresses.rs"]
mod listener_addresses;
#[path = "../outbound_dial.rs"]
mod outbound_dial;
