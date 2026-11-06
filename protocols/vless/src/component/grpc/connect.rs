use super::factory::Outbound;
use zero_platform_tokio::contract::{
    context::OpenContext,
    stream::{BoxStream, ConnectRequest, Connected, StreamOutbound},
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;
mod ordinary;
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
impl Outbound {
    async fn borrow(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let session = self.admission.check(&context)?;
        if request.destination.port == 0 {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let options = &self.options.prepared.protocol;
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
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        let io = self.acquire(&context).await?;
        // Extract the real logical controller before protocol submission can
        // suspend. Failed/cancelled submission stops this exact worker record;
        // the same persistent pool retains its physical original and Scope.
        let (io, control, original) = io.into_controlled_parts()?;
        // Cached physical acquisition happened in its owning pool's Service
        // task. The actual calling root still needs that original's endpoint
        // facts for this Business/Service invocation; observation grants no IO.
        context.upstream.observe_carrier_origin(&original);
        let connected = super::super::connect::submit(
            options,
            &self.admission,
            session,
            context,
            zero_platform_tokio::TcpRelayStream::new(super::super::carrier::Carrier::new(io)),
            bytes,
        )
        .await?;
        Ok(Connected {
            io: StreamLoan::new(connected.io, control, original),
            peer_confirmation: connected.peer_confirmation,
        })
    }
    pub(super) async fn acquire(
        &self,
        context: &OpenContext,
    ) -> Result<zero_transport::grpc::NativeGrpcStream, ChannelError> {
        let session = self.admission.check(context)?;
        let opening = self.options.prepared.pool.open_observed(
            self.pool,
            zero_transport::grpc::Demand {
                deadline: context.deadline,
                cancel: context.cancel.clone(),
                binding: context.upstream.binding().clone(),
            },
            context.upstream.observed_pending_carrier(),
        );
        tokio::select! {biased;
         _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::NotSubmitted)),
         result=opening=>result.map_err(|failure|self.admission.attach(failure,session)),
        }
    }
}
