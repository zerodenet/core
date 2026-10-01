//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../support/mod.rs"]
mod support;

#[path = "../core_capabilities.rs"]
mod core_capabilities;
#[path = "../export.rs"]
mod export;
#[path = "../inventory.rs"]
mod inventory;
#[path = "../management_idle.rs"]
mod management_idle;
#[path = "../policy_reload_rollback.rs"]
mod policy_reload_rollback;
#[path = "../port_conflict.rs"]
mod port_conflict;
#[path = "../reload_reconcile.rs"]
mod reload_reconcile;
#[path = "../session_observability.rs"]
mod session_observability;
#[path = "../shutdown.rs"]
mod shutdown;
#[path = "../stats.rs"]
mod stats;
