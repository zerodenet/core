use alloc::vec::Vec;
use core::future::Future;

use crate::Error;
use zero_traits::AsyncSocket;

mod multiplex;
pub use multiplex::{InboundDatagramMultiplexer, InboundStreamMultiplexer};

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
}
