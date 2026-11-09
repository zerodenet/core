//! Packet-path bridge facade.
//!
//! The root stays as a facade so send-path dispatch, recv-loop decoding, and
//! waiter queue management do not regrow into one mixed implementation bucket.

mod dispatch;
mod recv;
mod waiter;

pub(super) use dispatch::dispatch_via_entry;
pub(super) use recv::recv_loop;
pub(super) use waiter::Waiters;
