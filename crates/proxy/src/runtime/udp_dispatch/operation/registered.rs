use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
use crate::runtime::udp_dispatch::{UdpDispatch, UpstreamTrackedStart};
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};

pub(crate) struct RegisteredAssociationUdpOperation<T> {
    pub(crate) tag: String,
    pub(crate) server: String,
    pub(crate) port: u16,
    pub(crate) resume: T,
}

impl<T> PreparedUdpFlowOperation for RegisteredAssociationUdpOperation<T>
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
            execute_registered_association_operation(
                dispatch,
                session,
                payload,
                PreparedRegisteredAssociationOperation {
                    services: Some(ctx.runtime_services().with_outbound_io(&self.tag)),
                    tag: &self.tag,
                    server: &self.server,
                    port: self.port,
                    resume: self.resume,
                },
            )
            .await
        })
    }
}

struct PreparedRegisteredAssociationOperation<'a, T> {
    services: Option<UdpRuntimeServices>,
    tag: &'a str,
    server: &'a str,
    port: u16,
    resume: T,
}

async fn execute_registered_association_operation<T>(
    dispatch: &mut UdpDispatch,
    session: &Session,
    payload: &[u8],
    operation: PreparedRegisteredAssociationOperation<'_, T>,
) -> Result<FlowStartResult, FlowFailure>
where
    T: std::any::Any + Send + Sync + std::fmt::Debug,
{
    dispatch
        .start_tracked_upstream(UpstreamTrackedStart {
            services: operation.services,
            tag: operation.tag,
            session,
            server: operation.server,
            port: operation.port,
            resume: operation.resume,
            payload,
        })
        .await
}
