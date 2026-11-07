//! Standard views of the one actual linked peer graph.
mod datagram;
mod packet;
mod stream;
use super::{component::LinkedStackOptions, *};
use std::net::{IpAddr, SocketAddr};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::ResourceAccess, context::OpenContext, scope::TaskScopeIdentity, ChannelError,
        Impact, Reason, Stage,
    },
    controlled_carrier::CarrierOrigin,
};
#[derive(Clone)]
pub(in crate::adapters::wireguard::lifecycle) enum Graph {
    Linked(Arc<LinkedStackOptions>),
    Standalone(Arc<super::super::standalone::StandaloneStackOptions>),
}
pub(in crate::adapters::wireguard::lifecycle) struct Acquisition {
    _source: Option<zero_platform_tokio::contract::packet::PacketSourceAcquisition>,
    _native: Option<crate::runtime::packet_route::host::pause::Acquisition>,
}
impl Acquisition {
    fn source(actual: zero_platform_tokio::contract::packet::PacketSourceAcquisition) -> Self {
        Self {
            _source: Some(actual),
            _native: None,
        }
    }
    fn native(actual: crate::runtime::packet_route::host::pause::Acquisition) -> Self {
        Self {
            _source: None,
            _native: Some(actual),
        }
    }
}
pub(in crate::adapters::wireguard::lifecycle) struct Carrier {
    pub(in crate::adapters::wireguard::lifecycle) options: Graph,
    pub(in crate::adapters::wireguard::lifecycle) resources: Arc<dyn ResourceAccess>,
    pub(in crate::adapters::wireguard::lifecycle) scope: TaskScopeIdentity,
}
impl Carrier {
    fn plan(&self) -> &WireguardRawIpPlan {
        match &self.options {
            Graph::Linked(root) => &root.plan,
            Graph::Standalone(root) => &root.plan,
        }
    }
    fn handle(&self) -> Result<Arc<Self>, ChannelError> {
        match &self.options {
            Graph::Linked(root) => &root.carrier,
            Graph::Standalone(root) => &root.carrier,
        }
        .get()
        .and_then(std::sync::Weak::upgrade)
        .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
    fn origins_live(&self) -> Result<(), ChannelError> {
        match &self.options {
            Graph::Linked(_) => {
                self.origin()?;
            }
            Graph::Standalone(root) => {
                for index in 0..root.peers.len() {
                    root.original(index)?;
                }
            }
        }
        Ok(())
    }
    fn enter_source(
        &self,
    ) -> Result<zero_platform_tokio::contract::packet::PacketSourceAcquisition, ChannelError> {
        let original = self.origin()?;
        match original.logical_packet() {
            Some(logical) => {
                // A Source-issued software origin does not inherit native
                // close lineage. Its exact original Source admission gate is
                // separate and still seals opens before FlowUsers custody.
                let actual = logical
                    .source_admission()
                    .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
                zero_platform_tokio::controlled_carrier::CarrierOrigin::from_listener(
                    actual.clone(),
                )
                .try_enter_packet_source()
            }
            None => original.try_enter_packet_source(),
        }
    }
    fn acquisition(&self) -> Result<Vec<Acquisition>, ChannelError> {
        match &self.options {
            Graph::Linked(_) => Ok(vec![Acquisition::source(self.enter_source()?)]),
            Graph::Standalone(root) => (0..root.peers.len())
                .map(|index| root.acquisition(index).map(Acquisition::native))
                .collect(),
        }
    }
    fn peer_acquisition(&self, peer: usize) -> Result<Acquisition, ChannelError> {
        match &self.options {
            Graph::Linked(_) => Ok(Acquisition::source(self.enter_source()?)),
            Graph::Standalone(root) => root.acquisition(peer).map(Acquisition::native),
        }
    }
    fn origin_for(&self, target: IpAddr) -> Result<CarrierOrigin, ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        match &self.options {
            Graph::Linked(_) => self.origin(),
            Graph::Standalone(root) => Ok(CarrierOrigin::from_listener(
                root.original(
                    self.plan()
                        .peer_for_target(target)
                        .map_err(failure)?
                        .peer_index,
                )?,
            )),
        }
    }
    fn origin(&self) -> Result<CarrierOrigin, ChannelError> {
        self.resources.packet_carrier_origin()
    }
    fn device(
        &self,
        target: IpAddr,
    ) -> Result<(Arc<crate::runtime::raw_ip::SharedRawIpDevice>, IpAddr), ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let peer = self.plan().peer_for_target(target).map_err(failure)?;
        let device = match &self.options {
            Graph::Linked(root) => root
                .pending
                .peer_stack(peer.peer_index)
                .and_then(|owner| owner.device())
                .filter(|device| device.is_usable())
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?,
            Graph::Standalone(root) => root.device(peer.peer_index)?,
        };
        Ok((device, peer.local_ip))
    }
    async fn resolve(
        &self,
        target: &Destination,
        context: &OpenContext,
    ) -> Result<SocketAddr, ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let peers = context
            .upstream
            .resolve_packet_target(
                target.clone(),
                context.identity.clone(),
                zero_platform_tokio::contract::network::CarrierContext {
                    session: context.identity.session(),
                    deadline: context.deadline,
                    cancel: context.cancel.clone(),
                },
            )
            .await?;
        peers
            .into_iter()
            .find(|peer| self.plan().peer_for_target(peer.ip()).is_ok())
            .ok_or_else(|| error(Stage::Dial, Reason::InvalidConfig))
    }
    fn flow_borrow(
        &self,
        context: &OpenContext,
    ) -> Result<crate::runtime::raw_ip::PacketFlowBorrow, ChannelError> {
        let caller = context.upstream.packet_caller(
            &context.identity,
            &zero_platform_tokio::contract::network::CarrierContext {
                session: context.identity.session(),
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
        )?;
        let custody = context.upstream.packet_driver_custody(
            &context.identity,
            &zero_platform_tokio::contract::network::CarrierContext {
                session: context.identity.session(),
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
        )?;
        Ok(crate::runtime::raw_ip::PacketFlowBorrow {
            caller,
            custody,
            observer: context
                .observer
                .as_ref()
                .and_then(|observer| observer.packet_flow_io()),
        })
    }
    fn observe_peer(&self, target: IpAddr, context: &OpenContext) -> Result<(), ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let peer = self.plan().peer_for_target(target).map_err(failure)?;
        if let (Some(observer), Some(identity)) = (
            &context.observer,
            self.plan().peer_identity(peer.peer_index),
        ) {
            observer.packet_peer_selected(
                context.identity.session(),
                zero_core::component::PeerId(identity),
            );
        }
        Ok(())
    }
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
fn failure(source: EngineError) -> ChannelError {
    error(Stage::Prepare, Reason::Io).with_source(source)
}
