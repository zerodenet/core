use alloc::vec::Vec;
use core::future::Future;

use crate::Error;
use zero_traits::AsyncSocket;

mod control;
mod multiplex;
pub use control::InboundControlSession;
pub use multiplex::{
    InboundDatagramMultiplexer, InboundRouteMultiplexer, InboundStreamMultiplexer,
    InboundTransportMultiplexer,
};

pub trait InboundClientResponse<S>: Send + Sync
where
    S: AsyncSocket,
{
    fn send_ok(&self, client: &mut S) -> impl Future<Output = Result<(), Error>> + Send;

    fn send_blocked(&self, client: &mut S) -> impl Future<Output = Result<(), Error>> + Send;

    fn send_upstream_failure(
        &self,
        client: &mut S,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait InboundFallbackCapture {
    type Stream;

    fn into_fallback_replay_parts(self) -> (Self::Stream, Vec<u8>);
}

pub trait InboundFallbackReplay: Send + Sized {
    type Stream;

    fn selected_route(&self) -> Option<zero_traits::FallbackRoute> {
        None
    }

    fn replay_to<'a, W>(
        self,
        upstream: &'a mut W,
    ) -> impl Future<Output = Result<Self::Stream, W::Error>> + Send + 'a
    where
        Self: 'a,
        W: AsyncSocket + Send + 'a;
}

pub enum InboundRouteAccept<R, F> {
    Route(R),
    Fallback(F),
    Control(alloc::boxed::Box<dyn InboundControlSession>),
}

/// Removes handshake recording while preserving an owning protocol's stream codec.
/// The byte counters describe transport traffic consumed before this handoff.
pub trait InboundRecording {
    type Stream;
    fn into_unrecorded(self) -> (Self::Stream, u64, u64);
}
