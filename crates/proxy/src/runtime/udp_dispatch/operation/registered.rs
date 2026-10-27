//! Execute a prepared endpoint through the compiled standard Datagram role.
use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::{UdpDispatch, UpstreamTrackedStart};
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};
use std::{future::Future, pin::Pin};
use zero_core::Session;
#[derive(Clone)]
pub(crate) struct RegisteredAssociationUdpOperation {
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
}
impl PreparedUdpFlowOperation for RegisteredAssociationUdpOperation {
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            dispatch
                .start_tracked_upstream(UpstreamTrackedStart {
                    services: Some(ctx.runtime_services().with_outbound_io(&self.tag)),
                    tag: &self.tag,
                    server: &self.server,
                    port: self.port,
                    session,
                    payload,
                })
                .await
        })
    }
}
