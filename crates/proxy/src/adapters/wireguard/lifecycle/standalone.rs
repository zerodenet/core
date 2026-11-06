//! One standard host slot owns each standalone peer's existing wire and stack.
mod component;
mod preparation;
mod resolve;
use super::*;
use crate::runtime::{
    packet_route::host::pause::ReadPause,
    raw_ip::{EndpointStackOwner, RawIpStackOptions},
};
pub(super) use preparation::Candidate;
use std::{net::SocketAddr, num::NonZeroUsize, sync::OnceLock};
use zero_core::component::{ComponentId, ComponentInstanceId, NetworkGeneration};
use zero_platform_tokio::{
    contract::{
        component::{ComponentSpec, ResourceAccess, ResourceDeclaration, ResourceOwner, RoleKind},
        network::UpstreamBinding,
        scope::TaskScopeIdentity,
        ChannelError, Impact, Reason, Stage,
    },
    controlled_datagram::provider::NativeDatagramOrigin,
};

pub(super) const NAMESPACE: &str = "actual.assembly.wireguard.standalone.peers";
pub(in crate::adapters::wireguard) struct StandaloneStackOptions {
    pub(super) plan: Arc<WireguardRawIpPlan>,
    pub(super) spec: ComponentSpec,
    pub(super) binding: UpstreamBinding,
    pub(super) resource: OnceLock<ComponentId>,
    pub(super) carrier: OnceLock<std::sync::Weak<super::recipe::carrier::Carrier>>,
    pub(super) peers: Vec<Peer>,
    state: Mutex<State>,
}
pub(super) struct Peer {
    owner: Arc<EndpointStackOwner>,
    original: OnceLock<Arc<crate::runtime::raw_ip::HostedRawIpWireCarrier>>,
    pause: OnceLock<Arc<ReadPause>>,
}
struct State {
    scope: Option<TaskScopeIdentity>,
    dependencies: Option<Arc<[ComponentInstanceId]>>,
    resources: Option<Arc<dyn ResourceAccess>>,
    endpoints: Option<Vec<SocketAddr>>,
    stacks: Vec<RawIpStackOptions>,
    attempted: bool,
    stopped: bool,
}
impl StandaloneStackOptions {
    fn new(
        outbound: &OutboundConfig,
        plan: Arc<WireguardRawIpPlan>,
        options: RawIpStackOptions,
        network_generation: NetworkGeneration,
        dependencies: Vec<ComponentId>,
        count: usize,
    ) -> Result<Self, EngineError> {
        if !matches!(
            outbound.protocol,
            OutboundProtocolConfig::Wireguard {
                inbound_tag: None,
                outer_udp_proxy: None,
                ..
            }
        ) {
            return Err(invalid(
                "standalone host recipe requires its actual plain WireGuard outbound",
            ));
        }
        if count > plan.peer_count() || count > crate::runtime::raw_ip::MAX_RAW_IP_DEVICES {
            return Err(invalid(
                "standalone peer count exceeds the actual device bound",
            ));
        }
        let mut resources = Vec::with_capacity(count * 2);
        for _ in 0..count {
            let datagram = resources.len();
            resources.push(ResourceDeclaration::BoundOutboundDatagram {
                max_sockets: NonZeroUsize::MIN,
                max_bindings: NonZeroUsize::MIN,
            });
            resources.push(ResourceDeclaration::PacketDatagramReads {
                datagram,
                workspaces: [NonZeroUsize::new(65_536).unwrap(); 2],
            });
        }
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"zero.wireguard.standalone.graph.v1");
        for encoded in [
            serde_json::to_vec(&outbound.tag),
            serde_json::to_vec(&outbound.protocol),
            serde_json::to_vec(&outbound.dial),
            serde_json::to_vec(&outbound.udp),
        ] {
            let encoded =
                encoded.map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            hash.update((encoded.len() as u64).to_le_bytes());
            hash.update(encoded);
        }
        hash.update(
            options.identity(
                super::super::protocol_identity(&outbound.protocol)
                    .ok_or_else(|| invalid("standalone protocol identity is unavailable"))?,
            ),
        );
        hash.update(network_generation.0.to_le_bytes());
        hash.update((count as u64).to_le_bytes());
        for dependency in &dependencies {
            hash.update(dependency.0.to_le_bytes());
        }
        let task_limit = count
            .checked_mul(EndpointStackOwner::TASK_LIMIT)
            .map(NonZeroUsize::new)
            .ok_or_else(|| invalid("standalone actual task bound overflow"))?;
        Ok(Self {
            plan,
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: vec![RoleKind::Packet, RoleKind::Stream, RoleKind::Datagram],
                dependencies,
                static_identity: hash.finalize().to_vec().into(),
                resources,
                task_limit,
            },
            binding: UpstreamBinding {
                prefix: Vec::new(),
                dial: outbound.dial.to_policy(),
                network_generation,
            },
            resource: OnceLock::new(),
            carrier: OnceLock::new(),
            peers: (0..count)
                .map(|_| Peer {
                    owner: EndpointStackOwner::suspended(),
                    original: OnceLock::new(),
                    pause: OnceLock::new(),
                })
                .collect(),
            state: Mutex::new(State {
                scope: None,
                dependencies: None,
                resources: None,
                endpoints: None,
                stacks: vec![options; count],
                attempted: false,
                stopped: false,
            }),
        })
    }
    fn reusable_in(
        &self,
        preparation: &crate::component::ResourcePreparation<'_>,
        dependencies: &[ComponentInstanceId],
    ) -> bool {
        let (stopped, actual, scope) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (
                state.stopped,
                state.dependencies.clone(),
                state.scope.clone(),
            )
        };
        !stopped
            && actual
                .as_ref()
                .is_none_or(|actual| actual.as_ref() == dependencies)
            && preparation.can_reuse_shared_resource(NAMESPACE, &self.spec, scope.as_ref())
    }
    pub(super) fn device(&self, peer: usize) -> Result<Arc<SharedRawIpDevice>, ChannelError> {
        self.peers
            .get(peer)
            .and_then(|peer| peer.owner.device())
            .filter(|device| device.is_usable())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
    pub(super) fn original(&self, peer: usize) -> Result<NativeDatagramOrigin, ChannelError> {
        self.peers
            .get(peer)
            .and_then(|peer| peer.original.get())
            .and_then(|wire| wire.original())
            .filter(|original| !original.is_retired())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
    pub(super) fn acquisition(
        &self,
        peer: usize,
    ) -> Result<crate::runtime::packet_route::host::pause::Acquisition, ChannelError> {
        self.peers
            .get(peer)
            .and_then(|peer| peer.pause.get())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .try_enter()
    }
    fn revoke(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stopped = true;
        for peer in &self.peers {
            peer.owner.revoke();
        }
    }
    fn needs_cleanup(&self) -> bool {
        let resources = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.scope.is_none() {
                return false;
            } // Pure cold metadata only.
            state.resources.clone()
        };
        if self.peers.iter().any(|peer| !peer.owner.is_drained()) {
            return true;
        }
        resources.as_ref().is_none_or(|resources| {
            (0..self.peers.len()).any(|index| !resources.bound_datagram_retired(index * 2))
        })
    }
}
fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
fn failure(stage: Stage, source: EngineError) -> ChannelError {
    error(stage, Reason::Io).with_source(source)
}

impl WireguardAdapter {
    pub(crate) async fn pause_standalone_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<crate::runtime::packet_route::host::NativeDevicePaused>, EngineError> {
        let roots = {
            let current = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            current
                .values()
                .filter_map(|profile| profile.graph.as_ref())
                .map(|root| {
                    root.upgrade()
                        .ok_or_else(|| invalid("actual standalone factory owner was released"))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut pauses = Vec::new();
        for root in roots {
            for peer in &root.peers {
                let pause = peer
                    .pause
                    .get()
                    .ok_or_else(|| invalid("actual standalone wire checkpoint is unavailable"))?;
                pauses.push(pause.pause(deadline).await.map_err(std::io::Error::other)?);
            }
        }
        Ok(pauses)
    }
}
