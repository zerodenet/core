use super::factory::Outbound;
use super::*;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::{
    contract::{
        context::OpenContext,
        lifecycle::BoxChannelControl,
        stream::{BoxStream, ConnectRequest, Connected, StreamOutbound},
        DeliveryState,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};
use zero_transport::runner::GuardedCall;

mod datagram;

struct Ordinary(StreamLoan);
impl AsyncRead for Ordinary {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.io).poll_read(cx, bytes)
    }
}
impl AsyncWrite for Ordinary {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0.io).poll_shutdown(cx)
    }
}

impl Outbound {
    pub(super) async fn acquire_parts(
        &self,
        context: &OpenContext,
    ) -> Result<
        (
            zero_platform_tokio::TcpRelayStream,
            BoxChannelControl,
            CarrierOrigin,
        ),
        ChannelError,
    > {
        let crate::component::preconnect::Opened {
            io: stream,
            control,
            original,
        } = super::factory::Shared::acquire_transport(&self.shared, context).await?;
        let encrypted = self.shared.options.plan.encrypt(stream);
        let io = zero_platform_tokio::contract::context::with_deadline(context.deadline, encrypted)
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Timeout).with_source(source))?
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
        context.upstream.observe_carrier_origin(&original);
        Ok((io, control, original))
    }
    async fn borrow(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let call = GuardedCall::enter(&self.admission, &context, None)?;
        let options = &self.shared.options.protocol;
        if request.destination.port == 0 || context.upstream.binding().dial != options.dial {
            return Err(call.error(
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let bytes = options
            .stream_request(&request, &context)
            .map_err(|source| {
                call.error(
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
            })?;
        let session = call.session();
        let (io, control, original) = self.acquire_parts(&context).await?;
        let connected = crate::component::connect::submit(
            options,
            &self.admission,
            session,
            context,
            io,
            bytes,
        )
        .await?;
        Ok(Connected {
            io: StreamLoan::new(connected.io, control, original),
            peer_confirmation: connected.peer_confirmation,
        })
    }
}

impl StreamOutbound for Outbound {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let connected = self.borrow(request, context).await?;
        Ok(Connected {
            io: Box::pin(Ordinary(connected.io)),
            peer_confirmation: connected.peer_confirmation,
        })
    }
    async fn connect_carrier(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        self.borrow(request, context).await
    }
}

impl super::factory::Shared {
    pub(in crate::component) async fn acquire_transport(
        shared: &PacketMetadataArc<Self>,
        context: &OpenContext,
    ) -> Result<crate::component::preconnect::Opened, ChannelError> {
        let (io, control, original) = shared.handle.open_unobserved_parts(context).await?;
        Ok(crate::component::preconnect::Opened {
            io,
            control,
            original,
        })
    }
}
