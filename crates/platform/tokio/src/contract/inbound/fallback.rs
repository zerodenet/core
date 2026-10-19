use tokio::io::AsyncWriteExt;
use zero_traits::FallbackRoute;

use super::super::{
    stream::{BoxStream, StreamIo},
    BoxFuture, ChannelError, Impact, Reason, Stage,
};

pub struct FallbackStreams {
    pub inbound: BoxStream,
    pub upstream: BoxStream,
}

pub trait AcceptedFallback: Send {
    fn route(&self) -> &FallbackRoute;
    fn replay(
        self: Box<Self>,
        upstream: BoxStream,
    ) -> BoxFuture<'static, Result<FallbackStreams, ChannelError>>;
}

/// Capture must already be bounded by the protocol while reading the handshake.
/// This handoff checks that bound again, and replays the captured prefix once.
pub struct CapturedFallback<S> {
    route: FallbackRoute,
    prefix: Vec<u8>,
    io: S,
}
impl<S> CapturedFallback<S> {
    pub fn new(
        route: FallbackRoute,
        prefix: Vec<u8>,
        io: S,
        max_capture: usize,
    ) -> Result<Self, ChannelError> {
        if prefix.len() > max_capture {
            return Err(ChannelError::new(
                Stage::Handshake,
                Reason::Capacity,
                Impact::Session,
            ));
        }
        Ok(Self { route, prefix, io })
    }
}
impl<S: StreamIo + 'static> AcceptedFallback for CapturedFallback<S> {
    fn route(&self) -> &FallbackRoute {
        &self.route
    }
    fn replay(
        self: Box<Self>,
        mut upstream: BoxStream,
    ) -> BoxFuture<'static, Result<FallbackStreams, ChannelError>> {
        Box::pin(async move {
            upstream.write_all(&self.prefix).await.map_err(|source| {
                let mut error =
                    ChannelError::new(Stage::Send, Reason::Io, Impact::Session).with_source(source);
                error.delivery = super::super::DeliveryState::Unknown;
                error
            })?;
            Ok(FallbackStreams {
                inbound: Box::pin(self.io),
                upstream,
            })
        })
    }
}
