use crate::adapters::mieru::MieruAdapter;
use crate::protocol_registry::{UdpFlowResult, UdpRelayResult};
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_dispatch::operation::{
    ManagedStreamPacketBridgePlan, ManagedStreamPacketUdpOperation,
    PreparedManagedStreamPacketOperation, PreparedUdpFlowOperation,
};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::managed::{
    ManagedStreamConnectorFlow, ManagedTupleUdpFlowConnection, ManagedTupleUdpResume,
    ManagedTupleUdpResumeConnector,
};

mod datagram_relay;

#[async_trait::async_trait]
impl ManagedTupleUdpResumeConnector for ::mieru::transport::MieruManagedUdpFlowResume {
    type ConnectorFlow = ::mieru::transport::MieruManagedUdpConnectorFlow;
    type Connection = ::mieru::udp::MieruUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "mieru_establish";
    const RELAY_UPSTREAM_STAGE: &'static str = "mieru_relay_upstream";
    const RELAY_ESTABLISH_STAGE: &'static str = "mieru_relay_establish";
    const RELAY_SEND_STAGE: &'static str = "mieru_relay_send";

    fn connector_flow(&self, _server: &str, _port: u16, session_id: u64) -> Self::ConnectorFlow {
        ::mieru::transport::MieruManagedUdpFlowResume::connector_flow(self, session_id)
    }

    async fn open_direct(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        _session: &zero_core::Session,
    ) -> Result<Self::Connection, zero_engine::EngineError> {
        let factory = services.outbound_datagram_socket_factory();
        self.open_direct_connection(
            move |server, port| {
                let services = services.clone();
                let server = server.to_owned();
                async move { services.connect_upstream(&server, port).await }
            },
            factory,
        )
        .await
        .map_err(zero_engine::EngineError::from)
    }

    async fn open_relay(
        &self,
        _services: crate::protocol_registry::UpstreamConnectServices,
        stream: crate::transport::TcpRelayStream,
        _session: &zero_core::Session,
        _tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, zero_engine::EngineError> {
        self.open_relay_connection(stream)
            .await
            .map_err(zero_engine::EngineError::from)
    }

    async fn open_lazy_relay(
        &self,
        _services: crate::protocol_registry::UpstreamConnectServices,
        carrier: LazyTcpRelayCarrier<'_>,
        _session: &zero_core::Session,
        _tls_server_name: Option<&str>,
    ) -> Result<Self::Connection, zero_engine::EngineError> {
        let generation = carrier.generation();
        let relay_identity = carrier.identity().to_owned();
        let mut carrier = Some(carrier);
        self.open_pooled_relay_connection(generation, &relay_identity, move || {
            let carrier = carrier.take();
            async move {
                let carrier = carrier.ok_or_else(|| {
                    zero_engine::EngineError::Io(std::io::Error::other(
                        "Mieru TCP relay carrier factory was reused",
                    ))
                })?;
                carrier.open().await
            }
        })
        .await
        .map_err(zero_engine::EngineError::from)
    }
}

impl From<::mieru::transport::MieruManagedUdpConnectorFlow> for ManagedStreamConnectorFlow {
    fn from(flow: ::mieru::transport::MieruManagedUdpConnectorFlow) -> Self {
        let (cache_key, requires_relay_upstream) = flow.into_parts();
        Self::new(cache_key, requires_relay_upstream)
    }
}

#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for ::mieru::udp::MieruUdpFlowConnection {
    fn is_closed(&self) -> bool {
        ::mieru::udp::MieruUdpFlowConnection::is_closed(self)
    }

    async fn send(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, zero_engine::EngineError> {
        ::mieru::udp::MieruUdpFlowConnection::send(self, target, port, payload)
            .await
            .map_err(|error| zero_engine::EngineError::Io(std::io::Error::other(error.to_string())))
    }

    fn subscribe_responses(
        &self,
    ) -> tokio::sync::broadcast::Receiver<(zero_core::Address, u16, Vec<u8>)> {
        ::mieru::udp::MieruUdpFlowConnection::subscribe_responses(self)
    }

    fn closed_message(&self) -> &'static str {
        "mieru upstream closed"
    }
}

fn runtime_flow_plan_parts(
    parts: (
        String,
        String,
        u16,
        ::mieru::transport::MieruManagedUdpFlowResume,
    ),
) -> (
    String,
    String,
    u16,
    ManagedTupleUdpResume<::mieru::transport::MieruManagedUdpFlowResume>,
) {
    let (tag, server, port, resume) = parts;
    (tag, server, port, ManagedTupleUdpResume::new(resume))
}

struct PreparedMieruUdpRelay {
    plan: ::mieru::transport::MieruManagedUdpFlowPlan,
    datagram_carrier: bool,
}

impl<'a> PreparedUdpRelayOperation<'a> for PreparedMieruUdpRelay {
    fn requires_datagram_carrier(&self) -> bool {
        self.datagram_carrier
    }

    fn uses_lazy_stream_carrier(&self) -> bool {
        !self.datagram_carrier
    }

    fn bind_final_hop(
        self: Box<Self>,
        carrier: crate::transport::RelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        if self.datagram_carrier {
            return Err(FlowFailure {
                stage: "udp_relay_datagram_carrier",
                error: zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Mieru native UDP requires a datagram relay carrier",
                )),
                upstream: None,
            });
        }
        Ok(Box::new(ManagedStreamPacketUdpOperation {
            operation: PreparedManagedStreamPacketOperation::RelayFinalHop {
                plan: ManagedStreamPacketBridgePlan::from_parts(
                    runtime_flow_plan_parts(self.plan.into_parts()),
                    true,
                ),
                carrier,
            },
            // Tuple relay connectors require the narrow runtime network services.
            needs_proxy: true,
        }))
    }

    fn bind_datagram_carrier(
        self: Box<Self>,
        carrier: PreparedDatagramRelayCarrier,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        if !self.datagram_carrier {
            return Err(FlowFailure {
                stage: "udp_relay_datagram_carrier",
                error: zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Mieru TCP underlay cannot bind a datagram relay carrier",
                )),
                upstream: None,
            });
        }
        Ok(datagram_relay::bind(self.plan, carrier))
    }

    fn bind_lazy_stream_carrier(
        self: Box<Self>,
        carrier: LazyTcpRelayCarrier<'a>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        if self.datagram_carrier {
            return Err(FlowFailure {
                stage: "udp_relay_lazy_stream_carrier",
                error: zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Mieru UDP underlay cannot bind a stream relay carrier",
                )),
                upstream: None,
            });
        }
        Ok(Box::new(ManagedStreamPacketUdpOperation {
            operation: PreparedManagedStreamPacketOperation::LazyRelayFinalHop {
                plan: ManagedStreamPacketBridgePlan::from_parts(
                    runtime_flow_plan_parts(self.plan.into_parts()),
                    true,
                ),
                carrier: Box::new(carrier),
            },
            // Tuple relay connectors require the narrow runtime network services.
            needs_proxy: true,
        }))
    }
}

impl MieruAdapter {
    pub(super) fn udp_operations<'a>(
        &self,
        leaf: ::mieru::transport::MieruTransportLeaf,
    ) -> (UdpFlowResult<'a>, UdpRelayResult<'a>) {
        let datagram_carrier = leaf.uses_udp_carrier();
        let direct = Box::new(ManagedStreamPacketUdpOperation {
            operation: PreparedManagedStreamPacketOperation::Direct {
                plan: ManagedStreamPacketBridgePlan::from_parts(
                    runtime_flow_plan_parts(leaf.clone().udp_flow_plan(false).into_parts()),
                    false,
                ),
            },
            needs_proxy: true,
        });
        let relay = Box::new(PreparedMieruUdpRelay {
            plan: leaf.udp_flow_plan(true),
            datagram_carrier,
        });
        (Ok(direct), Ok(relay))
    }
}
