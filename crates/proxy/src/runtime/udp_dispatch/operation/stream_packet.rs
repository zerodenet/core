use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedUdpFlowOperation;
use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_flow::managed::bridge::{
    start_direct_managed_stream_packet, start_relay_managed_stream_packet,
    ManagedStreamPacketRelay, ManagedStreamPacketStartBridge,
};
use crate::runtime::udp_flow::result::{FlowFailure, FlowStartResult};
use crate::transport::RelayCarrier;

#[derive(Debug, Clone)]
pub(crate) struct ManagedStreamPacketBridgePlan<T> {
    tag: String,
    server: String,
    port: u16,
    resume: T,
    relay_chain: bool,
}

impl<T> ManagedStreamPacketBridgePlan<T> {
    pub(crate) fn from_parts(parts: (String, String, u16, T), relay_chain: bool) -> Self {
        let (tag, server, port, resume) = parts;
        Self {
            tag,
            server,
            port,
            resume,
            relay_chain,
        }
    }
}

pub(crate) struct ManagedStreamPacketUdpOperation<'op, T> {
    pub(crate) operation: PreparedManagedStreamPacketOperation<'op, T>,
    pub(crate) needs_proxy: bool,
}

impl<'op, T> PreparedUdpFlowOperation for ManagedStreamPacketUdpOperation<'op, T>
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
            let services = self.needs_proxy.then(|| ctx.runtime_services());
            execute_managed_stream_packet_operation(
                dispatch,
                services,
                session,
                payload,
                self.operation,
            )
            .await
        })
    }
}

pub(crate) enum PreparedManagedStreamPacketOperation<'op, T> {
    Direct {
        plan: ManagedStreamPacketBridgePlan<T>,
    },
    RelayFinalHop {
        plan: ManagedStreamPacketBridgePlan<T>,
        carrier: RelayCarrier,
    },
    LazyRelayFinalHop {
        plan: ManagedStreamPacketBridgePlan<T>,
        carrier: LazyTcpRelayCarrier<'op>,
    },
}

async fn execute_managed_stream_packet_operation<T>(
    dispatch: &mut UdpDispatch,
    services: Option<UdpRuntimeServices>,
    session: &Session,
    payload: &[u8],
    operation: PreparedManagedStreamPacketOperation<'_, T>,
) -> Result<FlowStartResult, FlowFailure>
where
    T: std::any::Any + Send + Sync + std::fmt::Debug,
{
    let mut context = dispatch.flow_start_context();
    match operation {
        PreparedManagedStreamPacketOperation::Direct { plan } => {
            debug_assert!(!plan.relay_chain);
            let services =
                services.expect("direct managed stream operation requires runtime services");
            start_direct_managed_stream_packet(
                &mut context,
                ManagedStreamPacketStartBridge::direct(
                    services,
                    &plan.tag,
                    session,
                    (&plan.server, plan.port),
                    plan.resume,
                    payload,
                ),
            )
            .await
        }
        PreparedManagedStreamPacketOperation::RelayFinalHop { plan, carrier } => {
            debug_assert!(plan.relay_chain);
            start_relay_managed_stream_packet(
                &mut context,
                ManagedStreamPacketStartBridge::relay(
                    services,
                    &plan.tag,
                    session,
                    ManagedStreamPacketRelay {
                        carrier: carrier.into(),
                        tls_server_name: None,
                    },
                    (&plan.server, plan.port),
                    plan.resume,
                    payload,
                ),
            )
            .await
        }
        PreparedManagedStreamPacketOperation::LazyRelayFinalHop { plan, carrier } => {
            debug_assert!(plan.relay_chain);
            start_relay_managed_stream_packet(
                &mut context,
                ManagedStreamPacketStartBridge::lazy_relay(
                    services,
                    &plan.tag,
                    session,
                    carrier,
                    (&plan.server, plan.port),
                    plan.resume,
                    payload,
                ),
            )
            .await
        }
    }
}
