use super::PreparedUdpFlowOperation;
use crate::{
    protocol_registry::UdpAdapterContext,
    runtime::{
        udp_dispatch::UdpDispatch,
        udp_flow::{
            registered::LogicalConnection,
            result::{FlowFailure, FlowStartResult},
        },
    },
};
use std::{future::Future, pin::Pin, sync::Arc};
use zero_core::Session;

pub(crate) struct LogicalUdpOperation {
    pub(crate) tag: String,
    pub(crate) open:
        Arc<dyn Fn(&Session) -> Result<LogicalConnection, zero_core::Error> + Send + Sync>,
}
impl PreparedUdpFlowOperation for LogicalUdpOperation {
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        _ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let connection = (self.open)(session).map_err(|error| FlowFailure {
                stage: "udp_logical_connect",
                error: error.into(),
                upstream: None,
            })?;
            dispatch
                .flow_start_context()
                .start_logical(self.tag, session, payload, connection)
                .await
        })
    }
}
