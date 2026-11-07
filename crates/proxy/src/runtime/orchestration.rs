//! Proxy orchestration facade.
//!
//! The root stays as a facade so startup, reload coordination, task loop
//! control, and runtime logging do not regrow into one implementation bucket.

mod apply;
mod completion;
#[cfg(feature = "raw-ip-runtime")]
mod devices;
mod endpoint;
mod lifecycle;
mod logging;
mod pending_dns;
mod reconcile;
#[cfg(feature = "raw-ip-runtime")]
pub(in crate::runtime) use reconcile::budget_barrier::packet_budget_changed;
pub(in crate::runtime) use reconcile::budget_barrier::{
    carrier_budget_changed, source_total_resources, ss_source_budget_changed,
};
mod startup;
mod state;
pub(super) mod statistics;
#[cfg(test)]
mod tests;

pub(super) use lifecycle::run_until;
