use crate::inventory::PreparedTcpRelayChain;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
use crate::runtime::udp_flow::packet_path::PacketPathFlowBinding;
use crate::runtime::udp_flow::packet_path_chain::PacketPathStartRequest;
use crate::transport::RelayCarrier;

pub(crate) trait PreparedUdpRelayOperation<'a>: Send {
    fn requires_datagram_carrier(&self) -> bool {
        false
    }

    fn uses_lazy_stream_carrier(&self) -> bool {
        false
    }

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

    fn bind_datagram_carrier(
        self: Box<Self>,
        _carrier: PreparedDatagramRelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Err(FlowFailure {
            stage: "udp_relay_datagram_carrier",
            error: zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "datagram relay carrier is unsupported for this outbound",
            )),
            upstream: None,
        })
    }

    fn bind_lazy_stream_carrier(
        self: Box<Self>,
        _carrier: LazyTcpRelayCarrier<'a>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        Err(FlowFailure {
            stage: "udp_relay_lazy_stream_carrier",
            error: zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "lazy stream relay carrier is unsupported for this outbound",
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
    DatagramFinalHop {
        carrier: PreparedDatagramRelayCarrier,
        operation: Box<dyn PreparedUdpRelayOperation<'a> + 'a>,
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
            Self::DatagramFinalHop { carrier, operation } => {
                operation
                    .bind_datagram_carrier(carrier)?
                    .execute(dispatch, ctx, session, payload)
                    .await
            }
            Self::FinalHop { prefix, operation } => {
                if operation.uses_lazy_stream_carrier() {
                    let carrier = ctx
                        .runtime_services()
                        .prepare_lazy_tcp_relay_carrier(prefix);
                    return operation
                        .bind_lazy_stream_carrier(carrier)?
                        .execute(dispatch, ctx, session, payload)
                        .await;
                }
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
