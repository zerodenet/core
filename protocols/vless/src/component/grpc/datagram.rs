//! Fixed-target VLESS UDP framing over the same actual pooled gRPC controller.
use super::super::datagram::{self, Control, Rx, Tx};
use super::factory::Outbound;
use tokio::io::AsyncWriteExt;
use zero_core::request::DatagramTargets;
use zero_platform_tokio::contract::{
    context::{ConnectFact, OpenContext},
    datagram::{DatagramOutbound, DatagramRequest, DatagramSession},
    ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::runner::GuardedCall;
impl DatagramOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let call = GuardedCall::enter(&self.admission, &context, None)?;
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(call.error(
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let mut prepared = datagram::Prepared::new(
            self.datagram_budget.as_ref(),
            &context.cancel,
            &self.admission,
            target,
        )?;
        let target = target.clone();
        let bytes = prepared
            .request(&target, &self.options.protocol().uuid)
            .map_err(|source| {
                call.error(
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
            })?;
        let raw = self.acquire(&call).await?;
        let super::super::preconnect::Opened {
            io,
            control,
            original,
        } = raw;
        // Cached physical acquisition happened in its owning pool's Service
        // task. The actual calling root still needs that original's endpoint
        // facts for this Business/Service invocation; observation grants no IO.
        context.upstream.observe_carrier_origin(&original);
        let mut io = io;
        call.run_io(Stage::Handshake, DeliveryState::Unknown, async {
            io.write_all(&bytes).await?;
            io.flush().await
        })
        .await?;
        let facts = call.facts();
        facts.record(ConnectFact::CarrierReady);
        facts.record(ConnectFact::RequestSubmitted);
        facts.protocol_traffic(0, bytes.len() as u64);
        let session = call.session();
        drop(bytes);
        // The peer may wait for the first UDP payload before replying. The
        // same Rx validates/accounts its real header and confirms the peer;
        // open must return the actual Tx before awaiting that response.
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
