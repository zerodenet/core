use crate::inventory::PreparedTcpRelayChain;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
use crate::runtime::udp_flow::packet_path::PacketPathFlowBinding;
use crate::runtime::udp_flow::packet_path_chain::PacketPathStartRequest;
use crate::transport::RelayCarrier;

pub(crate) trait PreparedUdpRelayOperation<'a>: Send {
    fn needs_two_streams(&self) -> bool {
        false
    }

    fn bind_final_hop(
        self: Box<Self>,
        carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure>;

    fn bind_two_stream(
        self: Box<Self>,
        post_carrier: RelayCarrier,
        get_carrier: RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let _ = (post_carrier, get_carrier);
        Err(FlowFailure {
            stage: "udp_relay_two_stream",
            error: zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "udp two-stream relay is unsupported for this outbound",
            )),
            upstream: None,
        })
    }
}

pub(crate) enum PreparedUdpRelayChain<'a> {
    PacketPath {
        flow_binding: PacketPathFlowBinding,
        request: Box<PacketPathStartRequest<'a>>,
    },
    FinalHop {
        prefix: PreparedTcpRelayChain<'a>,
        operation: Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
    },
    TwoStream {
        post_prefix: PreparedTcpRelayChain<'a>,
        get_prefix: PreparedTcpRelayChain<'a>,
        operation: Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
    },
}

impl PreparedUdpRelayChain<'_> {
    pub(crate) async fn execute(
        self,
        dispatch: &mut UdpDispatch,
        ctx: UdpAdapterContext<'_>,
        session: &zero_core::Session,
        payload: &[u8],
    ) -> Result<FlowStartResult, FlowFailure> {
        match self {
            Self::PacketPath {
                flow_binding,
                request,
            } => {
                let sent = dispatch.send_packet_path_chain(ctx, *request).await?;
                Ok(FlowStartResult::Flow {
                    outbound: Box::new(UdpDispatch::datagram_chain_flow_outbound(flow_binding)),
                    tx_bytes: sent as u64,
                })
            }
            Self::FinalHop { prefix, operation } => {
                let carrier = ctx
                    .runtime_services()
                    .dispatch_prepared_tcp_relay_carrier(prefix)
                    .await
                    .map_err(flow_failure_from_tcp_outbound)?;
                operation
                    .bind_final_hop(carrier)?
                    .execute(dispatch, ctx, session, payload)
                    .await
            }
            Self::TwoStream {
                post_prefix,
                get_prefix,
                operation,
            } => {
                let services = ctx.runtime_services();
                let post_carrier = services
                    .dispatch_prepared_tcp_relay_carrier(post_prefix)
                    .await
                    .map_err(flow_failure_from_tcp_outbound)?;
                let get_carrier = services
                    .dispatch_prepared_tcp_relay_carrier(get_prefix)
                    .await
                    .map_err(flow_failure_from_tcp_outbound)?;
                operation
                    .bind_two_stream(post_carrier, get_carrier)?
                    .execute(dispatch, ctx, session, payload)
                    .await
            }
        }
    }
}

fn flow_failure_from_tcp_outbound(failure: crate::transport::TcpOutboundFailure) -> FlowFailure {
    FlowFailure {
        stage: failure.stage,
        error: failure.error,
        upstream: failure.upstream_endpoint.map(|endpoint| *endpoint),
    }
}
