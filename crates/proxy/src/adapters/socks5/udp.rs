use ::socks5::transport::Socks5ManagedUdpPacketPathPlan;
use zero_engine::EngineError;

use crate::adapters::socks5::Socks5Adapter;
use crate::protocol_registry::{ClaimedUdpFlowLeaf, ClaimedUdpPacketPathLeaf};
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;
use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::registered::UpstreamAssociationHandler;

mod handler;
mod packet_path;
mod upstream_association;

struct Socks5PacketPathOperation {
    plan: Socks5ManagedUdpPacketPathPlan,
}

struct ClaimedSocks5PacketPathLeaf {
    plan: Socks5ManagedUdpPacketPathPlan,
}

struct ClaimedSocks5UdpLeaf {
    leaf: ::socks5::transport::Socks5TransportLeaf,
}

impl<'a> ClaimedUdpFlowLeaf<'a> for ClaimedSocks5UdpLeaf {
    fn prepare_udp_flow(
        &self,
        _source_dir: Option<&std::path::Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, FlowFailure> {
        let (tag, server, port, resume) = self.leaf.clone().udp_flow_plan().into_parts();
        Ok(Box::new(
            crate::runtime::udp_dispatch::operation::RegisteredAssociationUdpOperation {
                tag,
                server,
                port,
                resume,
            },
        ))
    }
}

impl<'a> ClaimedUdpPacketPathLeaf<'a> for ClaimedSocks5PacketPathLeaf {
    fn prepare_udp_packet_path(&self) -> Option<Box<dyn PreparedUdpPacketPathOperation>> {
        Some(Box::new(Socks5PacketPathOperation {
            plan: self.plan.clone(),
        }))
    }
}

impl PreparedUdpPacketPathOperation for Socks5PacketPathOperation {
    fn carrier_descriptor(
        &self,
    ) -> Option<crate::runtime::udp_flow::packet_path::PacketPathCarrierDescriptor> {
        Some(packet_path::carrier_descriptor(self.plan.clone()))
    }

    fn build_carrier<'a>(
        &'a self,
        services: crate::protocol_registry::UdpNetworkServices,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        std::sync::Arc<
                            dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier,
                        >,
                        EngineError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        let plan = self.plan.clone();
        Box::pin(async move { packet_path::build(services, plan).await })
    }
}

pub(crate) fn upstream_association_handler() -> Box<dyn UpstreamAssociationHandler> {
    handler::upstream_association_handler()
}

impl Socks5Adapter {
    pub(super) fn claim_udp_flow_leaf_impl<'a>(
        &self,
        leaf: ::socks5::transport::Socks5TransportLeaf,
    ) -> Box<dyn ClaimedUdpFlowLeaf<'a> + 'a> {
        Box::new(ClaimedSocks5UdpLeaf { leaf })
    }

    pub(super) fn claim_udp_packet_path_leaf_impl<'a>(
        &self,
        leaf: ::socks5::transport::Socks5TransportLeaf,
    ) -> Option<Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>> {
        Some(Box::new(ClaimedSocks5PacketPathLeaf {
            plan: leaf.udp_packet_path_plan(),
        }))
    }
}
