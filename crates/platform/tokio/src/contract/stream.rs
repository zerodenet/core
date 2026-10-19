use std::{future::Future, pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use zero_core::request::Destination;

use super::{context::OpenContext, BoxFuture, ChannelError};

pub trait StreamIo: AsyncRead + AsyncWrite + Send {}
impl<T: AsyncRead + AsyncWrite + Send + ?Sized> StreamIo for T {}
pub type BoxStream = Pin<Box<dyn StreamIo>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerConfirmation {
    Confirmed,
    Deferred,
}

pub struct Connected<S> {
    pub io: S,
    /// Local availability and remote protocol confirmation are separate facts.
    pub peer_confirmation: PeerConfirmation,
}

#[derive(Debug, Clone)]
pub struct ConnectRequest {
    pub destination: Destination,
}

pub trait StreamOutbound: Send + Sync + 'static {
    type Stream: StreamIo + 'static;

    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> impl Future<Output = Result<Connected<Self::Stream>, ChannelError>> + Send;
}

pub trait DynStreamOutbound: Send + Sync {
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>>;
}

struct Erased<T>(T);
impl<T: StreamOutbound> DynStreamOutbound for Erased<T> {
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>> {
        Box::pin(async move {
            context.business_facts()?;
            let connected = self.0.connect(request, context).await?;
            Ok(Connected {
                io: Box::pin(connected.io) as BoxStream,
                peer_confirmation: connected.peer_confirmation,
            })
        })
    }
}

pub fn erase_stream(outbound: impl StreamOutbound) -> Arc<dyn DynStreamOutbound> {
    Arc::new(Erased(outbound))
}
