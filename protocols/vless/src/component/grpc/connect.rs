use super::factory::Outbound;
use zero_platform_tokio::contract::{
    context::OpenContext,
    stream::{BoxStream, ConnectRequest, Connected, StreamOutbound},
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;
use zero_transport::runner::GuardedCall;
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
        let call = GuardedCall::enter(&self.admission, &context, None)?;
        if request.destination.port == 0 {
            return Err(call.error(
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let options = self.options.protocol();
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
        let io = self.acquire(&call).await?;
        // Extract the real logical controller before protocol submission can
        // suspend. Failed/cancelled submission stops this exact worker record;
        // the same persistent pool retains its physical original and Scope.
        let super::super::preconnect::Opened {
            io,
            control,
            original,
        } = io;
        // Cached physical acquisition happened in its owning pool's Service
        // task. The actual calling root still needs that original's endpoint
        // facts for this Business/Service invocation; observation grants no IO.
        context.upstream.observe_carrier_origin(&original);
        let connected =
            super::super::connect::submit(options, &self.admission, session, context, io, bytes)
                .await?;
        Ok(Connected {
            io: StreamLoan::new(connected.io, control, original),
            peer_confirmation: connected.peer_confirmation,
        })
    }
    pub(super) async fn acquire(
        &self,
        call: &GuardedCall<'_>,
    ) -> Result<super::super::preconnect::Opened, ChannelError> {
        let context = call.context();
        let opening = async {
            match &self.options.source {
                super::options::Source::Carrier { pool, .. } => pool.open(self.pool, context).await,
                super::options::Source::Preconnect { pool, .. } => {
                    pool.open(self.pool, context).await
                }
                super::options::Source::Grpc(prepared) => {
                    let io = prepared
                        .pool
                        .open_observed(
                            self.pool,
                            zero_transport::grpc::Demand {
                                deadline: context.deadline,
                                cancel: context.cancel.clone(),
                                binding: context.upstream.binding().clone(),
                            },
                            context.upstream.observed_pending_carrier(),
                        )
                        .await?;
                    let (io, control, original) = io.into_controlled_parts()?;
                    Ok(super::super::preconnect::Opened {
                        io: zero_platform_tokio::TcpRelayStream::new(
                            super::super::carrier::Carrier::new(io),
                        ),
                        control,
                        original,
                    })
                }
            }
        };
        let mut opened = call
            .run(Stage::Handshake, DeliveryState::NotSubmitted, opening)
            .await
            .map_err(|mut failure| {
                if failure.reason == Reason::Timeout {
                    failure.delivery = DeliveryState::Unknown;
                }
                failure
            })?
            .map_err(|failure| call.attach(failure))?;
        if let Some(encryption) = &self.options.protocol().encryption {
            opened.io = call
                .run_io(
                    Stage::Handshake,
                    DeliveryState::Unknown,
                    encryption.handshake(opened.io),
                )
                .await?
                .into_relay();
        }
        Ok(opened)
    }
}
