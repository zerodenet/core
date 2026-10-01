//! Proxy orchestration facade.
//!
//! The root stays as a facade so startup, reload coordination, task loop
//! control, and runtime logging do not regrow into one implementation bucket.

#[cfg(feature = "raw-ip-runtime")]
mod devices;
mod endpoint;
mod lifecycle;
mod logging;
mod state;
mod statistics;
#[cfg(test)]
mod tests;

pub(super) use lifecycle::run_until;
