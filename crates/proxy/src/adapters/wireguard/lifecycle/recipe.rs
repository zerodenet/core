//! Cold linked-client recipe. Only BuildNew may resolve or register originals.
use super::{inbound, invalid, PendingLinkedEndpoint, WireguardAdapter, WireguardRawIpPlan};
use crate::{
    protocol_registry::UpstreamConnectServices,
    runtime::{
        raw_ip::{EndpointStackRecipe, RawIpStackOptions},
        udp_dispatch::packet_owner::PacketCarrierDemand,
    },
};
use std::{net::IpAddr, sync::Arc};
use tokio::sync::{mpsc, watch};
use zero_config::{InboundConfig, OutboundConfig, OutboundProtocolConfig};
use zero_core::{component::NetworkGeneration, Address};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    context::{with_deadline, RequestIdentity},
    network::UpstreamBinding,
};

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
        })
    }
    pub(super) fn pending(&self) -> Arc<PendingLinkedEndpoint> {
        self.pending.clone()
    }
    pub(super) fn binding(&self) -> UpstreamBinding {
        self.binding.clone()
    }
    fn check_staged(&self) -> Result<(), EngineError> {
        let map = self.map.lock().unwrap_or_else(|error| error.into_inner());
        if !map
            .as_ref()
            .and_then(|map| map.get(&self.tag))
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &self.pending))
        {
            return Err(invalid(
                "linked resource owner was not installed in this actual candidate",
            ));
        }
        if !self.pending.is_usable() {
            return Err(invalid("linked candidate resource owner is retired"));
        }
        Ok(())
    }
    /// The host invokes this only after actual affected old listener and peer
    /// resources closed. The same candidate owns every partial result on Drop.
    pub(super) async fn start(
        self,
        upstream: &UpstreamConnectServices,
        demand: PacketCarrierDemand,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        demand.validate()?;
        if !matches!(&demand.identity, RequestIdentity::Service(_))
            || demand.binding != self.binding
        {
            return Err(invalid(
                "linked BuildNew requires its actual Service identity and complete root binding",
            ));
        }
        self.check_staged()?;
        let cancel_owner = self
            .pending
            .peer_carrier(0)
            .ok_or_else(|| invalid("linked peer declaration is empty"))?;
        let pending = self.pending.clone();
        let deadline = demand.context.deadline;
        let caller_cancel = demand.context.cancel.clone();
        let building = async move {
            if upstream.egress_generation() != self.binding.network_generation.0 {
                return Err(invalid("linked candidate network generation is stale"));
            }
            let mut endpoints = Vec::with_capacity(self.plan.peer_count());
            for peer in 0..self.plan.peer_count() {
                let Some((host, port)) = self.plan.peer_endpoint(peer) else {
                    endpoints.push(None);
                    continue;
                };
                let address = match host.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
                    Ok(IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
                    Err(_) => Address::Domain(host.to_owned()),
                };
                endpoints.push(Some(
                    upstream
                        .resolve_node_address(&address, port, "WireGuard peer endpoint")
                        .await?,
                ));
            }
            demand.validate()?;
            self.check_staged()?;
            let mut devices = Vec::with_capacity(self.stacks.len());
            for (peer, recipe) in self.stacks.into_iter().enumerate() {
                devices.push(
                    pending
                        .peer_stack(peer)
                        .ok_or_else(|| invalid("linked client owner declaration is missing"))?
                        .start(recipe)?,
                );
            }
            demand.validate()?;
            let link = Arc::new(inbound::LinkedEndpoint::new(
                self.identity,
                self.binding.network_generation.0,
                pending.listen().clone(),
                pending.live(),
                devices,
                endpoints,
                vec![None; self.plan.peer_count()],
                self.sender,
                self.packets,
            ));
            pending.install(link.clone())?;
            Ok(link)
        };
        tokio::select! {
            biased;
            _ = cancel_owner.cancelled() => Err(EngineError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted, "linked candidate was stopped"))),
            _ = caller_cancel.cancelled() => Err(EngineError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted, "linked BuildNew caller cancelled"))),
            result = with_deadline(deadline, building) => result.map_err(|source| EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source)))?,
        }
    }
}
#[cfg(test)]
mod tests;
