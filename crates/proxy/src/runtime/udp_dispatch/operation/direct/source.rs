//! Only the actual host Direct operation extracts this narrow policy authority.
use super::DirectUdpFlowOperation;
use crate::runtime::{
    current_services::CurrentServiceSource,
    inbound_owners::{HostPeerBinding, PeerResourceContext},
    udp_ingress::UdpIngressRuntime,
};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
use zero_core::{component::NetworkGeneration, Network, Session};
use zero_platform_tokio::{
    contract::{
        component::DatagramBindingAuthorization, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    EgressInterfaceControl, EgressSelection,
};
use zero_traits::DialPolicy;

pub(crate) struct DirectSocketSource {
    host: Weak<CurrentServiceSource>,
    tag: Option<String>,
    policy: DialPolicy,
    policy_generation: u64,
    preferred_port: Option<u16>,
    egress: EgressInterfaceControl,
    peer: Arc<HostPeerBinding>,
}
impl std::fmt::Debug for DirectSocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectSocketSource")
            .field("policy_generation", &self.policy_generation)
            .finish_non_exhaustive()
    }
}
impl DirectUdpFlowOperation {
    /// Cold preparation only. The caller must be the host's admitted Direct
    /// operation; protocol request fields never contain a policy or selection.
    pub(crate) async fn socket_source(
        &self,
        host: Weak<CurrentServiceSource>,
        runtime: &UdpIngressRuntime,
        session: &Session,
        context: &PeerResourceContext,
    ) -> Result<DirectSocketSource, ChannelError> {
        context.binding().wait_bound().await?;
        let actual = host.upgrade().ok_or_else(unavailable)?;
        let execution = runtime.services().packet_path_execution().tcp();
        let snapshot = actual.engine().runtime_snapshot();
        // This is an actual Arc-backed snapshot identity, not an equal config
        // generation number from another Engine or an old caller snapshot.
        if !std::ptr::eq(execution.snapshot(), snapshot.as_ref()) {
            return Err(error(
                Reason::StalePlan,
                "Direct operation is not from the current host",
            ));
        }
        if execution.components.as_ref().ok().is_none_or(|plan| {
            actual
                .protocols()
                .ok()
                .and_then(|protocols| protocols.current_components(snapshot.config()))
                .is_none_or(|current| !Arc::ptr_eq(plan, &current))
        }) {
            return Err(error(
                Reason::StalePlan,
                "Direct operation has not been published by this host",
            ));
        }
        let ingress = runtime.peer_ingress().map_err(|cause| {
            error(Reason::InvalidConfig, "actual Direct ingress is missing").with_source(cause)
        })?;
        if ingress != context.ingress()
            || !runtime.peer_resources().is_some_and(|installed| {
                std::sync::Arc::ptr_eq(&installed.binding(), &context.binding())
            })
        {
            return Err(error(
                Reason::InvalidConfig,
                "Direct source belongs to another actual peer",
            ));
        }
        // Read the actual host registry once when extracting a new epoch. A
        // SessionHandle::new or an arbitrary nonzero numeric ID is not proof.
        let inbound = execution
            .ingress
            .as_ref()
            .map(|identity| identity.tag.as_str());
        let admitted = actual.engine().active_sessions().into_iter().any(|active| {
            active.id == session.id
                && active.network == Network::Udp
                && session.network == Network::Udp
                && active.inbound_tag.as_deref() == inbound
                && active.target == session.target
                && active.port == session.port
                && active.protocol == session.protocol
                && active.auth == session.auth
        });
        if !admitted || context.binding().cancellation().is_cancelled() {
            return Err(error(
                Reason::Revoked,
                "actual Direct session/peer admission is no longer live",
            ));
        }
        let source = DirectSocketSource {
            host,
            tag: self.policy_tag.clone(),
            policy: self.dial_policy.clone(),
            policy_generation: self.dial_generation,
            preferred_port: runtime.source_addr().map(|address| address.port()),
            egress: execution.upstream().egress_control(),
            peer: context.binding(),
        };
        source.check_current(NetworkGeneration(source.egress.generation()))?;
        Ok(source)
    }
}
impl DirectSocketSource {
    pub(crate) fn belongs_to_peer(&self, binding: &Arc<HostPeerBinding>) -> bool {
        Arc::ptr_eq(&self.peer, binding)
    }
    pub(crate) fn policy_identity(&self) -> crate::runtime::udp_socket::DirectUdpPolicy {
        crate::runtime::udp_socket::DirectUdpPolicy {
            tag: self.tag.clone(),
            dial_policy: self.policy.clone(),
            generation: self.policy_generation,
        }
    }
}
impl DatagramBindingAuthorization for DirectSocketSource {
    fn policy(&self) -> &DialPolicy {
        &self.policy
    }
    fn preferred_port(&self) -> Option<u16> {
        self.preferred_port
    }
    fn check_current(&self, expected: NetworkGeneration) -> Result<(), ChannelError> {
        if self.peer.cancellation().is_cancelled() {
            return Err(error(Reason::Revoked, "actual Direct peer stopped"));
        }
        let host = self.host.upgrade().ok_or_else(unavailable)?;
        if !host.engine().runtime_snapshot().direct_dial_policy_matches(
            self.tag.as_deref(),
            &self.policy,
            self.policy_generation,
        ) || self.egress.generation() != expected.0
        {
            return Err(error(
                Reason::StalePlan,
                "actual Direct policy or network generation changed",
            ));
        }
        Ok(())
    }
    fn select(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
    ) -> Result<EgressSelection, ChannelError> {
        self.check_current(expected)?;
        let selection = self
            .egress
            .select_for_peer_with_policy(peer, &self.policy)
            .map_err(|cause| {
                error(Reason::Io, "host Direct egress selection failed").with_source(cause)
            })?;
        self.check_current(expected)?;
        if selection.generation() != expected.0 {
            return Err(error(
                Reason::StalePlan,
                "Direct egress selection belongs to an old network",
            ));
        }
        Ok(selection)
    }
}
fn unavailable() -> ChannelError {
    error(Reason::Revoked, "actual Direct host is unavailable").with_source(std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        "proxy runtime has stopped",
    ))
}
fn error(reason: Reason, message: &'static str) -> ChannelError {
    let mut failure = ChannelError::new(Stage::Prepare, reason, Impact::Session)
        .with_source(std::io::Error::other(message));
    failure.delivery = DeliveryState::NotSubmitted;
    failure
}

#[cfg(test)]
pub(crate) mod tests;
