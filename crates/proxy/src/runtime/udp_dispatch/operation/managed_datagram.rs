use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::UdpAdapterContext;
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

// Only the deferred process socket path retains this executable operation.
pub(crate) struct ManagedDatagramUdpOperation<T> {
    pub(crate) plan: ManagedDatagramStartPlan<T>,
}

impl<T> PreparedUdpFlowOperation for ManagedDatagramUdpOperation<T>
where
    T: crate::runtime::udp_flow::managed::datagram_manager::ManagedDatagramSocketResumeConnector,
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
            let services = Some(ctx.runtime_services().with_outbound_io(&self.plan.tag));
            dispatch
                .start_transport_managed_socket(services, session, payload, self.plan)
                .await
        })
    }
}
