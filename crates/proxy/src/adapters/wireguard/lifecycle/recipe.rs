//! Cold linked-client recipe. Only BuildNew may resolve or register originals.
mod component;
mod invocation;
use super::{inbound, invalid, PendingLinkedEndpoint, WireguardAdapter, WireguardRawIpPlan};
use crate::{
    protocol_registry::UpstreamConnectServices,
    runtime::{
        raw_ip::{EndpointStackRecipe, RawIpStackOptions},
        udp_dispatch::packet_owner::PacketCarrierDemand,
    },
};
use component::{LinkedStackFactory, LinkedStackOptions};
pub(super) use invocation::LinkedInvocation;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use zero_config::{InboundConfig, OutboundConfig, OutboundProtocolConfig};
use zero_core::component::NetworkGeneration;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{network::UpstreamBinding, scope::TaskRegistrar};

pub(super) struct LinkedRecipeSettings {
    pub(super) identity: [u8; 32],
    pub(super) network_generation: NetworkGeneration,
    pub(super) stacks: Vec<RawIpStackOptions>,
}
pub(super) struct LinkedEndpointRecipe {
    tag: String,
    map: super::PendingEndpointMap,
    pending: Arc<PendingLinkedEndpoint>,
    identity: [u8; 32],
    binding: UpstreamBinding,
    plan: Arc<WireguardRawIpPlan>,
    stacks: Vec<EndpointStackRecipe>,
    sender: watch::Sender<mpsc::Sender<crate::runtime::raw_ip::EndpointPacket>>,
    packets: mpsc::Receiver<crate::runtime::raw_ip::EndpointPacket>,
    static_identity: Arc<[u8]>,
}
impl LinkedEndpointRecipe {
    /// No DNS, native handle, task registration or old receiver take occurs.
    /// Proxy carriers retain their legacy route until their reconnect/control
    /// graphs can keep originals through every fresh per-demand invocation.
    pub(super) fn prepare(
        adapter: &WireguardAdapter,
        outbound: &OutboundConfig,
        inbound: &InboundConfig,
        plan: Arc<WireguardRawIpPlan>,
        settings: LinkedRecipeSettings,
    ) -> Result<Self, EngineError> {
        match &outbound.protocol {
            OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(tag),
                outer_udp_proxy: None,
                ..
            } if tag == &inbound.tag => {}
            _ => {
                return Err(invalid(
                    "cold linked recipe requires its actual plain linked WireGuard outbound",
                ))
            }
        }
        if !settings.stacks.is_empty() && settings.stacks.len() != plan.peer_count() {
            return Err(invalid(
                "linked client stack declaration differs from its configured peers",
            ));
        }
        // Only the digest leaves this cold conversion, never the configuration
        // document, private keys, or a historical Engine/config owner.
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(b"zero.wireguard.linked.graph.v1");
        for bytes in [
            serde_json::to_vec(&outbound.tag),
            serde_json::to_vec(&outbound.protocol),
            serde_json::to_vec(&outbound.dial),
            serde_json::to_vec(&outbound.udp),
            serde_json::to_vec(&inbound.tag),
            serde_json::to_vec(&inbound.protocol),
            serde_json::to_vec(&inbound.listen),
        ] {
            let bytes = bytes.map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        hash.update(settings.identity);
        hash.update(settings.network_generation.0.to_le_bytes());
        hash.update((settings.stacks.len() as u64).to_le_bytes());
        for options in &settings.stacks {
            hash.update(options.identity(settings.identity));
        }
        let static_identity: Arc<[u8]> = Arc::from(hash.finalize().to_vec());
        let pending = Arc::new(PendingLinkedEndpoint::with_peer_slots(
            inbound.listen.clone(),
            adapter.passive_inbound_live(inbound)?,
            plan.peer_count(),
        )?);
        let (sender, packets) = mpsc::channel(256);
        let (sender, endpoint) = watch::channel(sender);
        let stacks = settings
            .stacks
            .into_iter()
            .enumerate()
            .map(|(peer, options)| {
                EndpointStackRecipe::prepare_checked(
                    plan.local_addresses(),
                    plan.mtu(),
                    peer,
                    endpoint.clone(),
                    options,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            tag: inbound.tag.clone(),
            map: adapter.pending_endpoints.clone(),
            pending,
            identity: settings.identity,
            binding: UpstreamBinding {
                prefix: Vec::new(),
                dial: outbound.dial.to_policy(),
                network_generation: settings.network_generation,
            },
            plan,
            stacks,
            sender,
            packets,
            static_identity,
        })
    }
    /// The host grants the actual declared stack workers, never an invented
    /// aggregate wrapper or a second runtime. None declares no stack tasks.
    pub(super) fn task_limit(&self) -> Result<Option<std::num::NonZeroUsize>, EngineError> {
        self.stacks
            .len()
            .checked_mul(crate::runtime::raw_ip::EndpointStackOwner::TASK_LIMIT)
            .map(std::num::NonZeroUsize::new)
            .ok_or_else(|| invalid("linked worker declaration overflow"))
    }
    pub(super) fn pending(&self) -> Arc<PendingLinkedEndpoint> {
        self.pending.clone()
    }
    pub(super) fn binding(&self) -> UpstreamBinding {
        self.binding.clone()
    }
    fn check_staged(&self) -> Result<(), EngineError> {
        Self::check_owner(&self.map, &self.tag, &self.pending)
    }
    fn check_owner(
        owner: &super::PendingEndpointMap,
        tag: &str,
        pending: &Arc<PendingLinkedEndpoint>,
    ) -> Result<(), EngineError> {
        let map = owner.lock().unwrap_or_else(|error| error.into_inner());
        if !map
            .as_ref()
            .and_then(|map| map.get(tag))
            .is_some_and(|candidate| Arc::ptr_eq(candidate, pending))
        {
            return Err(invalid(
                "linked resource owner was not installed in this actual candidate",
            ));
        }
        if !pending.is_usable() {
            return Err(invalid("linked candidate resource owner is retired"));
        }
        Ok(())
    }
    /// Split one pure candidate into the canonical graph owner and its short
    /// BuildNew invocation. No query budget or network service enters Options.
    pub(super) fn into_component(
        self,
        preparation: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<(zero_core::component::ComponentId, LinkedInvocation), EngineError> {
        let (options, invocation) = self.into_parts()?;
        let id = preparation
            .share(
                "actual.assembly.wireguard.linked.peers",
                Arc::new(LinkedStackFactory),
                options,
            )
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        Ok((id, invocation))
    }
    fn into_parts(mut self) -> Result<(Arc<LinkedStackOptions>, LinkedInvocation), EngineError> {
        let task_limit = self.task_limit()?;
        let stacks = std::mem::take(&mut self.stacks);
        let options = LinkedStackOptions::new(&self, stacks, task_limit);
        let peers = options.peer_count();
        Ok((
            Arc::new(options),
            LinkedInvocation {
                recipe: self,
                peers,
            },
        ))
    }
    /// Library bridge with explicit actual admission. Production canonical
    /// callers use into_component and the same slot's BuildContext.tasks.
    pub(super) async fn start(
        self,
        upstream: &UpstreamConnectServices,
        registrar: &TaskRegistrar,
        demand: PacketCarrierDemand,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        let (graph, invocation) = self.into_parts()?;
        graph.bind(registrar.downgrade())?;
        let resolved = invocation.resolve(upstream, demand).await?;
        graph.start_graph()?;
        resolved.install()
    }
}
#[cfg(test)]
mod tests;
