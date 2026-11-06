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
pub(super) struct Carrier {
    pub(super) options: Arc<LinkedStackOptions>,
    pub(super) resources: Arc<dyn ResourceAccess>,
    pub(super) scope: TaskScopeIdentity,
}
impl Carrier {
    fn origin(&self) -> Result<CarrierOrigin, ChannelError> {
        self.resources.packet_carrier_origin()
    }
    fn device(
        &self,
        target: IpAddr,
    ) -> Result<(Arc<crate::runtime::raw_ip::SharedRawIpDevice>, IpAddr), ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let peer = self.options.plan.peer_for_target(target).map_err(failure)?;
        let device = self
            .options
            .pending
            .peer_stack(peer.peer_index)
            .and_then(|owner| owner.device())
            .filter(|device| device.is_usable())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
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
            .find(|peer| self.options.plan.peer_for_target(peer.ip()).is_ok())
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
        Ok(crate::runtime::raw_ip::PacketFlowBorrow {
            caller,
            observer: context
                .observer
                .as_ref()
                .and_then(|observer| observer.packet_flow_io()),
        })
    }
    fn observe_peer(&self, target: IpAddr, context: &OpenContext) -> Result<(), ChannelError> {
        use crate::runtime::raw_ip::RawIpOutboundPlan;
        let peer = self.options.plan.peer_for_target(target).map_err(failure)?;
        if let (Some(observer), Some(identity)) = (
            &context.observer,
            self.options.plan.peer_identity(peer.peer_index),
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
