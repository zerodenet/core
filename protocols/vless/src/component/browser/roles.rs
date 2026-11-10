use super::factory::Outbound;
use super::*;
use zero_platform_tokio::{
    contract::{
        context::OpenContext,
        lifecycle::BoxChannelControl,
        stream::{BoxStream, ConnectRequest, Connected, StreamOutbound},
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};
mod datagram;
pub(super) mod logical;
mod ordinary;
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
        // The concrete logical-controller box is paid before Source task selection.
        let credit = self
            .shared
            .memory
            .reserve(PacketMetadataLayout::boxed::<logical::Logical>())
            .map_err(capacity)?;
        let loan = if self.shared.options.plan.browser_http().is_some() {
            super::factory::Shared::acquire_http(&self.shared, context).await?
        } else {
            super::factory::Shared::acquire_ws(&self.shared, context).await?
        };
        let original = loan.owner.original().clone();
        let stream = zero_transport::pooled_driver::ExclusiveIo::new(loan.io);
        let control = BoxChannelControl::from_prepaid(logical::Logical::Loan(loan.owner), credit)
            .map_err(|(source, _, _)| capacity(source))?;
        let encrypted = self
            .shared
            .options
            .plan
            .encrypt(zero_platform_tokio::TcpRelayStream::new(stream));
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
        let session = self.admission.check(&context)?;
        let options = &self.shared.options.protocol;
        if request.destination.port == 0 || context.upstream.binding().dial != options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                zero_platform_tokio::contract::DeliveryState::NotSubmitted,
            ));
        }
        let bytes = crate::outbound::build_tcp_request_to(
            &request.destination.address,
            request.destination.port,
            &options.uuid,
            options.flow,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    zero_platform_tokio::contract::DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
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
            io: Box::pin(ordinary::Ordinary(connected.io)),
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
