//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../browser_dialer.rs"]
mod browser_dialer;
#[path = "../h2_alps.rs"]
mod h2_alps;
#[path = "../http_upgrade.rs"]
mod http_upgrade;
#[path = "../ws_early_data.rs"]
mod ws_early_data;
#[path = "../ws_heartbeat.rs"]
mod ws_heartbeat;
#[path = "../ws_host.rs"]
mod ws_host;
