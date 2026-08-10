//! Proxy orchestration facade.
//!
//! The root stays as a facade so startup, reload coordination, task loop
//! control, and runtime logging do not regrow into one implementation bucket.

mod lifecycle;
mod logging;
mod state;
#[cfg(test)]
mod tests;

pub(super) use lifecycle::run_until;
