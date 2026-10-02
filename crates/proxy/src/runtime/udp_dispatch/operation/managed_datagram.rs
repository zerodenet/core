use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};

#[derive(Debug, Clone)]
pub(crate) struct ManagedDatagramStartPlan<T> {
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) resume: T,
}

impl<T> ManagedDatagramStartPlan<T> {
    pub(crate) fn from_parts(parts: (String, String, u16, T)) -> Self {
        let (tag, server, port, resume) = parts;
        Self {
            tag,
            server,
            port,
            resume,
        }
    }
}

pub(crate) struct ManagedDatagramUdpOperation<T> {
    pub(crate) plan: ManagedDatagramStartPlan<T>,
    pub(crate) needs_proxy: bool,
}

impl<T> PreparedUdpFlowOperation for ManagedDatagramUdpOperation<T>
where
    T: std::any::Any + Send + Sync + std::fmt::Debug,
{
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
            let services = self
                .needs_proxy
                .then(|| ctx.runtime_services().with_outbound_io(&self.plan.tag));
            execute_managed_datagram_operation(dispatch, services, session, payload, self.plan)
                .await
        })
    }
}

async fn execute_managed_datagram_operation<T>(
    dispatch: &mut UdpDispatch,
    services: Option<UdpRuntimeServices>,
    session: &Session,
    payload: &[u8],
    operation: ManagedDatagramStartPlan<T>,
) -> Result<FlowStartResult, FlowFailure>
where
    T: std::any::Any + Send + Sync + std::fmt::Debug,
{
    dispatch
        .start_transport_managed_datagram(services, session, payload, operation)
        .await
}
