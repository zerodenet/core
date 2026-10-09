//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../plan.rs"]
mod plan;
#[path = "../route_bypass.rs"]
mod route_bypass;
#[path = "../router.rs"]
mod router;
#[path = "../runtime_snapshot.rs"]
mod runtime_snapshot;

#[path = "../resolve_branches.rs"]
mod resolve_branches;
#[path = "../direct_dial.rs"]
mod direct_dial;
