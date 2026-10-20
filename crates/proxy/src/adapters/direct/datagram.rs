//! Direct delegates only to its bound upstream, retaining that session's owner.
use super::factory::{during_open, Outbound};
use std::pin::Pin;
use zero_platform_tokio::contract::{
    context::{with_deadline, ConnectFact, OpenContext},
    datagram::{DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::{ChannelControl, CloseReport, DynChannelControl},
    network::CarrierContext,
    ChannelError, CloseReason, DeliveryState, Reason, Stage,
};
pub(super) struct Control(Box<dyn DynChannelControl>);
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        self.0.cancel(reason);
    }
    async fn wait_closed(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        self.0.wait_closed(deadline).await
    }
}
impl DatagramOutbound for Outbound {
    type Tx = Pin<Box<dyn DatagramTx>>;
    type Rx = Pin<Box<dyn DatagramRx>>;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let session = self.state.check(&context)?;
        if context.upstream.binding().dial != self.options.policy {
            return Err(self.state.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let carrier = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let opened=tokio::select! { biased;
            _=self.state.cancelled()=>return Err(during_open(&self.state,session,Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(during_open(&self.state,session,Reason::Cancelled)),
            result=with_deadline(context.deadline,context.upstream.open_datagram(request,carrier))=>result.map_err(|source|during_open(&self.state,session,Reason::Timeout).with_source(source))?,
        }.map_err(|error|self.state.attach(error,session))?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::CarrierReady);
        }
        Ok(DatagramSession {
            tx: opened.tx,
            rx: opened.rx,
            control: Control(opened.control),
            limits: opened.limits,
        })
    }
}
