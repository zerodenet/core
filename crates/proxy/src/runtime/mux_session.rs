//! Mux sub-stream session facade.
//!
//! The root stays as a facade so session-loop lifecycle, exported request
//! models, and protocol-opened route dispatch do not collapse back into one
//! implementation bucket.

mod lifecycle;
mod model;
mod protocol;
#[cfg(test)]
mod tests;

pub(crate) use model::MuxSessionLoop;
pub(crate) use protocol::run_protocol_mux_session;
