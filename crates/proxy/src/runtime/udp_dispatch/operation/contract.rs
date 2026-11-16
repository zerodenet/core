use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};

pub(crate) trait PreparedUdpFlowOperation: Send {
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a;
}
