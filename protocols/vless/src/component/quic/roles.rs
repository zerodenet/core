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
        let initialized = entry::Generation::acquire(&self.generation, context).await?;
        let entry = initialized.entry.clone();
        let original = entry
            .origin
            .get()
            .cloned()
            .ok_or_else(|| error(Stage::Prepare, Reason::Internal))?;
        let relay = initialized.into_relay();
        let credit = entry
            .owners
            .state
            .lock()
            .await
            .logical_memory
            .take()
            .expect("one original prepaid logical control");
        let control = BoxChannelControl::from_prepaid(entry::LogicalControl(entry.clone()), credit)
            .map_err(|(source, _, _)| {
                error(Stage::Prepare, Reason::Capacity).with_source(source)
            })?;
        context.upstream.observe_carrier_origin(&original);
        Ok((relay, control, original))
    }
    async fn borrow(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let session = self.admission.check(&context)?;
        let options = &self.generation.options.protocol;
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
