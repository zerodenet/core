//! Related integration cases share one executable; feature gates stay on cases.
#![recursion_limit = "256"]

#[path = "../probe_outbound.rs"]
mod probe_outbound;
#[path = "../urltest_health_scope.rs"]
mod urltest_health_scope;
