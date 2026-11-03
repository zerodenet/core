use super::*;
use zero_platform_tokio::contract::datagram::{
    DatagramLimits, DatagramOutbound, DatagramRequest, DatagramSession,
};
use zero_transport::pooled_driver::datagram::{self, Control, Rx, Tx};
impl DatagramOutbound for Outbound {
    type Tx = Tx<crate::mux_pool::native::DatagramIo>;
    type Rx = Rx<crate::mux_pool::native::DatagramIo>;
    type Control = Control<crate::mux_pool::native::DatagramIo>;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
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
        datagram::session(
            opened.io,
            opened.usage,
            self.admission.clone(),
            &context,
            request,
            limits,
        )
    }
}
