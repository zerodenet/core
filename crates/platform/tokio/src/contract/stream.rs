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

    /// Explicit logical-carrier borrowing. A normal BoxStream cannot stand in
    /// for an original donor/control chain. Unmigrated roles deny before I/O.
    fn connect_carrier(
        &self,
        _request: ConnectRequest,
        _context: OpenContext,
    ) -> impl Future<
        Output = Result<Connected<crate::controlled_carrier::loan::StreamLoan>, ChannelError>,
    > + Send {
        async { Err(unsupported_carrier()) }
    }
}

pub trait DynStreamOutbound: Send + Sync {
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>>;
    fn connect_carrier(
        &self,
        _request: ConnectRequest,
        _context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<crate::controlled_carrier::loan::StreamLoan>, ChannelError>>
    {
        Box::pin(async { Err(unsupported_carrier()) })
    }
}

struct Erased<T>(T);
impl<T: StreamOutbound> DynStreamOutbound for Erased<T> {
    fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<crate::controlled_carrier::loan::StreamLoan>, ChannelError>>
    {
        Box::pin(async move {
            context.session_identity()?;
            self.0.connect_carrier(request, context).await
        })
    }
    fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> BoxFuture<'_, Result<Connected<BoxStream>, ChannelError>> {
        Box::pin(async move {
            context.session_identity()?;
            let connected = self.0.connect(request, context).await?;
            Ok(Connected {
                io: Box::pin(connected.io) as BoxStream,
                peer_confirmation: connected.peer_confirmation,
            })
        })
    }
}

fn unsupported_carrier() -> ChannelError {
    let mut error = ChannelError::new(
        super::Stage::Prepare,
        super::Reason::Unsupported,
        super::Impact::Component,
    );
    error.delivery = super::DeliveryState::NotSubmitted;
    error
}

pub fn erase_stream(outbound: impl StreamOutbound) -> Arc<dyn DynStreamOutbound> {
    Arc::new(Erased(outbound))
}
