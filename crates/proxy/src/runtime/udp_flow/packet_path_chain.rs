//! Datagram-over-packet-path manager for UDP relay chains.
//!
//! Models the relay pattern where the first hop (carrier) provides a raw
//! send/recv channel ([`PacketPathCarrier`]) and the final hop (datagram)
//! encodes its protocol datagrams through that channel ([`DatagramCodec`]).
//!
//! The root stays as a facade so start-path dispatch, snapshot forwarding, and
//! entry cache ownership do not regrow into one mixed implementation bucket.

mod bridge;
pub(crate) mod carriers;
mod entry;
mod key;
mod model;
mod receiver;
mod snapshot;
mod start;
mod state;

pub(crate) use model::{PacketPathCarrierRequest, PacketPathStartRequest};
pub(crate) use snapshot::SendWithSnapshotRequest;
pub(crate) use state::PacketPathManager;
