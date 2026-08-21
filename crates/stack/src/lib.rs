//! User-space network stack implementations.
//!
//! Provides [`UserNetworkStack`] — a pure-Rust TCP termination and UDP
//! forwarding stack that converts raw IP packets (from a TUN device or
//! similar) into `AsyncRead + AsyncWrite` streams and datagram I/O.
//!
//! # Trait hierarchy
//!
//! The stack traits are defined in `zero-traits`:
//! - [`TcpStack`] — feed raw packets → accept established connections
//! - [`UdpStack`] — feed raw packets → send/receive datagrams
//! - [`NetworkStack`] — bundles both
//!
//! # Architecture
//!
//! ```text
//!   raw IP packet
//!       │
//!       ▼
//!   TcpStack::feed()  ──►  internal state machine
//!       │                      │
//!       │               SYN-ACK / ACK / FIN
//!       │                      │
//!       │                      ▼
//!       │               outbound packet channel
//!       │
//!   TcpStack::accept()  ◄──  established connection
//!       │
//!       ▼
//!   serve_inbound()  (proxy kernel pipeline)
//! ```

pub mod fragment;
pub mod packet;
pub mod system;
pub mod tcp;
pub mod udp;

pub use fragment::{FragmentOutcome, FragmentReassembler, FragmentRejectReason};
pub use system::{SystemTcpStack, SystemUdpStack};
pub use tcp::{UserTcpStack, UserTcpStream};
pub use udp::UserUdpStack;

use std::sync::Arc;

use tokio::sync::mpsc;

use zero_traits::NetworkStack;

// ── User-space network stack ──────────────────────────────────────────

/// Outbound packet channel type.
#[allow(dead_code)]
type Outbound = mpsc::Sender<Vec<u8>>;

/// User-space network stack: TCP termination + UDP forwarding.
///
/// Construct with [`UserNetworkStack::new`], passing an outbound packet
/// channel.  The caller reads raw IP packets from a TUN device, feeds
/// them via [`TcpStack::feed`] / [`UdpStack::feed`], and drains outbound
/// packets from a separate channel back to the device.
pub struct UserNetworkStack {
    tcp: Arc<UserTcpStack>,
    udp: Arc<UserUdpStack>,
}

impl UserNetworkStack {
    /// Create a new user-space stack.
    ///
    /// `outbound` is a channel through which response packets
    /// (SYN-ACK, ACK, FIN, UDP responses) are sent back to the
    /// TUN device writer.
    ///
    /// `mss` is the TCP Maximum Segment Size advertised in SYN-ACK.
    pub fn new(outbound: mpsc::Sender<Vec<u8>>, mss: u16) -> Self {
        let mtu = usize::from(mss).saturating_add(60);
        Self {
            tcp: Arc::new(UserTcpStack::new(outbound.clone(), mss)),
            udp: Arc::new(UserUdpStack::new(outbound, mtu)),
        }
    }

    /// Split into individual stack references.
    pub fn into_parts(self) -> (Arc<UserTcpStack>, Arc<UserUdpStack>) {
        (self.tcp, self.udp)
    }
}

/// Convert an interface MTU to a TCP MSS that is safe for both IPv4 and IPv6.
pub fn tcp_mss_for_mtu(mtu: u16) -> u16 {
    // IPv6 header (40 bytes) + TCP header without options (20 bytes).
    mtu.saturating_sub(60)
}

impl NetworkStack for UserNetworkStack {
    type Tcp = UserTcpStack;
    type Udp = UserUdpStack;

    fn tcp(&self) -> &Self::Tcp {
        &self.tcp
    }

    fn udp(&self) -> &Self::Udp {
        &self.udp
    }
}
