//! Shadowsocks-owned UDP framing and bounded inbound state.
#[cfg(feature = "crypto")]
use alloc::vec::Vec;
#[cfg(feature = "crypto")]
use zero_core::Address;
#[cfg(feature = "crypto")]
use zero_core::{
    DatagramUdpResponder, Error, InboundDatagramUdpRelay, InboundUdpDispatch, ProtocolType,
    SessionAuth,
};
#[cfg(feature = "crypto")]
use zero_traits::{DatagramCodec, UdpDatagramFraming};

mod codec;
mod lifecycle;
mod model;
mod responder;
mod session;
mod state;
pub use codec::ShadowsocksInboundUdpCodec;
pub use model::*;

mod association;
