use super::*;
use zero_platform_tokio::contract::datagram::{
    DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession,
};
use zero_transport::pooled_driver::datagram::{self, Control, Rx, Tx};
impl DatagramOutbound for Outbound {
    type Tx = Tx<crate::mux_pool::native::DatagramIo>;
    type Rx = Rx<crate::mux_pool::native::DatagramIo>;
    type Control = Control<crate::mux_pool::native::DatagramIo>;
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::DatagramLoan, ChannelError> {
        let zero_core::request::DatagramTargets::Fixed(endpoint) = request.targets.clone() else {
            return Err(self.admission.error(
                context.session_identity()?,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ));
        };
        let association = request.association;
        let (session, original) = self.open_actual(request, context).await?;
        Ok(
            zero_platform_tokio::controlled_carrier::loan::DatagramLoan::new(
                zero_platform_tokio::contract::datagram::erase_session(session),
                original,
                association,
                endpoint,
            ),
        )
    }
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        self.open_actual(request, context)
            .await
            .map(|(session, _)| session)
    }
}
impl Outbound {
    async fn open_actual(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<
        (
            zero_transport::pooled_driver::datagram::PooledSession<
                crate::mux_pool::native::DatagramIo,
            >,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        ChannelError,
    > {
        let session = self.admission.check(&context)?;
        let leaf = self.selected_leaf(&context)?;
        let opening = leaf.open_native_mux_udp(context.upstream.binding().clone());
        let opened=tokio::select! {biased;
            _=self.admission.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _=context.cancel.cancelled()=>return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            opened=with_deadline(context.deadline,opening)=>opened.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source|self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if !context.upstream.observe_carrier_origin(&opened.origin) {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        let limits = DatagramLimits {
            max_payload: 16 * 1024,
            targets: request.targets.clone(),
            queued_bytes: 0,
        };
        let original = opened.origin;
        let session = datagram::session(
            opened.io,
            opened.usage,
            self.admission.clone(),
            &context,
            request,
            limits,
        )?;
        Ok((session, original))
    }
}
