//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../event_subscription.rs"]
mod event_subscription;
#[path = "../passive_relay_health.rs"]
mod passive_relay_health;
#[path = "../principal_flow_observability.rs"]
mod principal_flow_observability;
#[path = "../traffic_accounting.rs"]
mod traffic_accounting;
#[path = "../traffic_observation.rs"]
mod traffic_observation;
#[path = "../traffic_recovery.rs"]
mod traffic_recovery;
#[path = "../urltest_selection.rs"]
mod urltest_selection;
#[path = "../urltest_traffic_health.rs"]
mod urltest_traffic_health;

#[path = "../traffic_lifecycle.rs"]
mod traffic_lifecycle;
#[path = "../traffic_roles.rs"]
mod traffic_roles;
