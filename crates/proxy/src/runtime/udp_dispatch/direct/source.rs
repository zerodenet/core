//! Only host-selected Direct facts extract this actual provider authority.
use super::DirectDatagramTarget;
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
    caller: Caller,
}
#[derive(Clone)]
enum Caller {
    Peer(Arc<HostPeerBinding>),
    Accepted(zero_platform_tokio::AcceptedCaller),
}
impl Caller {
    fn check_open(&self) -> Result<(), ChannelError> {
        match self {
            Self::Peer(peer)
                if peer.cancellation().is_cancelled()
                    || peer.join_witness().is_none_or(|caller| {
                        caller.is_joined() || !caller.scope_identity().resource_open()
                    }) =>
            {
                Err(error(
                    Reason::Revoked,
                    "actual Direct peer stopped or joined",
                ))
            }
            Self::Peer(_) => Ok(()),
            Self::Accepted(caller) => caller.check_open(),
        }
    }
}
impl std::fmt::Debug for DirectSocketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectSocketSource")
            .field("policy_generation", &self.policy_generation)
            .finish_non_exhaustive()
    }
}
impl DirectDatagramTarget {
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
        self.source_for_caller(host, runtime, session, Caller::Peer(context.binding()))
    }
    pub(crate) fn accepted_socket_source(
        &self,
        host: Weak<CurrentServiceSource>,
        runtime: &UdpIngressRuntime,
        session: &Session,
    ) -> Result<DirectSocketSource, ChannelError> {
        let caller = runtime.accepted_caller().ok_or_else(|| {
            error(
                Reason::InvalidConfig,
                "actual accepted Direct caller is missing",
            )
        })?;
        self.source_for_caller(host, runtime, session, Caller::Accepted(caller.clone()))
    }
    fn source_for_caller(
        &self,
        host: Weak<CurrentServiceSource>,
        runtime: &UdpIngressRuntime,
        session: &Session,
        caller: Caller,
    ) -> Result<DirectSocketSource, ChannelError> {
        caller.check_open()?;
        let actual = host.upgrade().ok_or_else(unavailable)?;
        let execution = runtime.services().packet_path_execution().tcp();
        let snapshot = actual.engine().runtime_snapshot();
        // This is an actual Arc-backed snapshot identity, not an equal config
        // generation number from another Engine or an old caller snapshot.
        if !std::ptr::eq(execution.snapshot(), snapshot.as_ref()) {
            return Err(error(
                Reason::StalePlan,
                "Direct selection is not from the current host",
            ));
        }
        if execution.components.as_ref().ok().is_none_or(|plan| {
            actual
                .protocols()
                .ok()
                .and_then(|protocols| protocols.current_components(snapshot.resource_config()))
                .is_none_or(|current| !Arc::ptr_eq(plan, &current))
        }) {
            return Err(error(
                Reason::StalePlan,
                "Direct selection has not been published by this host",
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
        if !admitted {
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
            preferred_port: runtime.direct_preferred_port(),
            egress: execution.upstream().egress_control(),
            caller,
        };
        source.check_current(NetworkGeneration(source.egress.generation()))?;
        Ok(source)
    }
}
impl DirectSocketSource {
    pub(crate) fn belongs_to_task(
        &self,
        actual: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> bool {
        let caller = match &self.caller {
            Caller::Peer(peer) => peer.join_witness(),
            Caller::Accepted(caller) => caller.task_join_witness(),
        };
        caller.is_some_and(|caller| caller.same_registration(actual))
    }
    pub(crate) fn belongs_to_peer(&self, binding: &Arc<HostPeerBinding>) -> bool {
        matches!(&self.caller,Caller::Peer(peer) if Arc::ptr_eq(peer,binding))
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
        self.caller.check_open()?;
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
            // Let the actual bound provider record an unavailable route as a
            // failed native acquisition, with its same-owner no-native receipt.
            .describe_for_peer_with_policy(peer, &self.policy)
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
