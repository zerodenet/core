//! Fixed-target VLESS UDP framing over the same actual pooled gRPC controller.
use crate::component::browser::factory::Outbound;
use crate::component::datagram::{self, response, Control, Rx, Tx};
use tokio::io::AsyncWriteExt;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramOutbound, DatagramRequest, DatagramSession},
    ChannelError, DeliveryState, Reason, Stage,
};
impl DatagramOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.admission.check(&context)?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        if self.shared.options.protocol.flow.is_some()
            || target.port == 0
            || context.upstream.binding().dial != self.shared.options.protocol.dial
        {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let mut prepared = datagram::Prepared::new(
            Some(&datagram::Budget::new(
                self.shared.buffers.clone(),
                self.shared.memory.clone(),
            )),
            &context.cancel,
            &self.admission,
            target,
        )?;
        let target = target.clone();
        let bytes = prepared
            .request(&target, &self.shared.options.protocol.uuid)
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

        let (mut io, control, original) = self.acquire_parts(&context).await?;
        let handshake = async {
            io.write_all(&bytes).await?;
            io.flush().await
        };
        tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result=with_deadline(context.deadline,handshake)=>result.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, bytes.len() as u64);
        }
        drop(bytes);
        // The existing UDP tunnel waits for the actual VLESS response header;
        // gRPC establishment is not destination acceptance.
        let response = response::read(&mut io, &context, &self.admission, session).await?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::PeerConfirmed);
            observer.protocol_traffic(session, response as u64, 0);
        }
        let loan = zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(
            Box::pin(io),
            control,
            original.clone(),
        );
        Ok(datagram::session(
            Box::pin(loan.io),
            Some(loan.owner),
            Some(original),
            request,
            context,
            self.admission.clone(),
            session,
            target,
            prepared,
        ))
    }
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let association = request.association;
        let session = context.session_identity()?;
        let channel = self.open(request, context).await?;
        let original = channel.control.original().cloned().ok_or_else(|| {
            self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        })?;
        let DatagramTargets::Fixed(target) = &channel.limits.targets else {
            return Err(self.admission.error(
                None,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        };
        let target = target.clone();
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
                zero_platform_tokio::contract::datagram::erase_session(channel),
                original,
                association,
                target,
            ),
        )
    }
}
