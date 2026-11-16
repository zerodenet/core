//! Inspection decorates the same standard accepted channel. It owns no route,
//! native controller, task Scope or alternative UDP executor.
mod receive;
use crate::runtime::{sniff::SniffingPolicy, udp_ingress::UdpIngressRuntime};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    inbound::{AcceptedDatagram, BoxInboundDatagramSession, DatagramContinuity, ReplyOutcome},
    BoxFuture, ChannelError,
};
pub(super) fn datagram(
    request: Box<dyn AcceptedDatagram>,
    policy: SniffingPolicy,
    runtime: UdpIngressRuntime,
) -> Result<Box<dyn AcceptedDatagram>, EngineError> {
    let sniffing = crate::runtime::sniff::udp::UdpSniffingState::try_new(policy, runtime)?;
    Ok(Box::new(Inspected { request, sniffing }))
}
struct Inspected {
    request: Box<dyn AcceptedDatagram>,
    sniffing: crate::runtime::sniff::udp::UdpSniffingState,
}
impl AcceptedDatagram for Inspected {
    fn facts(&self) -> &zero_core::request::RequestFacts {
        self.request.facts()
    }
    fn continuity(&self) -> Option<DatagramContinuity> {
        self.request.continuity()
    }
    fn respond(
        self: Box<Self>,
        outcome: ReplyOutcome,
    ) -> BoxFuture<'static, Result<Option<BoxInboundDatagramSession>, ChannelError>> {
        Box::pin(async move {
            let Some(mut session) = self.request.respond(outcome).await? else {
                return Ok(None);
            };
            session.io.rx = Box::pin(receive::InspectedRx::new(session.io.rx, self.sniffing));
            Ok(Some(session))
        })
    }
}
