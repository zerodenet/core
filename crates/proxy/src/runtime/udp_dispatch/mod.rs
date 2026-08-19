//! Per-inbound-session UDP routing, flow selection, start, and forwarding.
//!
//! [`UdpDispatch`] is the UDP pipe state machine.
//! Inbound protocols create one dispatcher per UDP association/session, submit
//! packets through [`crate::runtime::pipe::UdpPipe`], and poll this dispatcher
//! for responses to deliver to the client.
//!
//! # Module layout
//!
//! - [`forward`](self::forward): re-dispatch packets on existing outbound flows
//! - [`crate::inventory::ProtocolInventory`]: resolved outbound selection and
//!   adapter-owned UDP flow preparation
//! - [`crate::runtime::udp_flow::registered`]: protocol handlers assembled by
//!   `register.rs` and their neutral runtime state
//! - [`crate::runtime::udp_flow::packet_path_chain`][]: generic
//!   datagram-over-packet-path manager for heterogeneous relay chains
//!
//! # UDP relay chain model
//!
//! The relay chain model is:
//!
//! ```text
//! previous hop provides a packet path (send/recv raw payloads)
//! next hop encodes its protocol datagram through that path
//! ```
//!
//! Adding new datagram-over-packet-path combinations requires implementing
//! [`UdpPacketPath`] and [`DatagramCodec`], not creating protocol-pair modules.
//!
//! # Usage
//!
//! ```ignore
//! let runtime = make_udp_ingress_runtime();
//! let mut dispatch = runtime.new_dispatch("inbound-tag").await?;
//!
//! // For each incoming packet:
//! UdpPipe::new(&mut dispatch).dispatch(input).await?;
//!
//! // Poll for responses in a select loop:
//! select! {
//!     recv = dispatch.direct_socket().recv_from_addr(&mut buf) => { /* direct response */ }
//!     resp = dispatch.poll_chain_response() => { /* chain response */ }
//!     // ...
//! }
//!
//! // Cleanup:
//! for completed in dispatch.finish_all() {
//!     log_completed_udp_flow(completed);
//! }
//! ```

// Sub-module declarations.

#[cfg(feature = "udp-runtime")]
mod backoff;
mod dispatch;
#[cfg(feature = "udp-runtime")]
mod failure;
#[cfg(feature = "udp-runtime")]
mod forward;
#[cfg(feature = "udp-runtime")]
mod lifecycle;
#[cfg(feature = "udp-runtime")]
mod managed;
#[cfg(feature = "udp-runtime")]
mod model;
mod outbound;
#[cfg(feature = "udp-runtime")]
mod packet_path;
pub(crate) use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};
#[cfg(feature = "upstream-association-runtime")]
pub(crate) use managed::UpstreamTrackedStart;
#[cfg(feature = "udp-runtime")]
pub(crate) use model::UdpDispatch;
pub(crate) use outbound::start_udp_resolved_outbound;
pub(crate) mod operation;
pub(crate) mod packet_path_operation;
pub(crate) mod relay;
