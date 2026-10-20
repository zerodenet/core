//! One actual BuildNew demand; none of these inputs enters the retained factory.
use super::*;
use std::net::{IpAddr, SocketAddr};
use zero_core::Address;
use zero_platform_tokio::contract::context::{with_deadline, RequestIdentity};

pub(in crate::adapters::wireguard::lifecycle) struct LinkedInvocation {
    pub(super) recipe: LinkedEndpointRecipe,
    pub(super) peers: usize,
}
pub(in crate::adapters::wireguard::lifecycle) struct ResolvedLinkedInvocation {
    recipe: LinkedEndpointRecipe,
    peers: usize,
    endpoints: Vec<Option<SocketAddr>>,
    demand: PacketCarrierDemand,
    egress: zero_platform_tokio::EgressInterfaceControl,
}
impl LinkedInvocation {
    /// Actual Node DNS and original caller deadline/cancellation precede graph
    /// start. Resolving a candidate cannot register one of its stack workers.
    pub(in crate::adapters::wireguard::lifecycle) async fn resolve(
        self,
        upstream: &UpstreamConnectServices,
        demand: PacketCarrierDemand,
    ) -> Result<ResolvedLinkedInvocation, EngineError> {
        demand.validate()?;
        if !matches!(&demand.identity, RequestIdentity::Service(_))
            || demand.binding != self.recipe.binding
        {
            return Err(invalid(
                "linked BuildNew requires its actual Service identity and complete root binding",
            ));
        }
        self.recipe.check_staged()?;
        let owner = self
            .recipe
            .pending
            .peer_carrier(0)
            .ok_or_else(|| invalid("linked peer declaration is empty"))?;
        let cancel = demand.context.cancel.clone();
        let resolution = async {
            if upstream.egress_generation() != self.recipe.binding.network_generation.0 {
                return Err(invalid("linked candidate network generation is stale"));
            }
            let mut endpoints = Vec::with_capacity(self.recipe.plan.peer_count());
            for peer in 0..self.recipe.plan.peer_count() {
                let Some((host, port)) = self.recipe.plan.peer_endpoint(peer) else {
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
            Ok(endpoints)
        };
        let endpoints = tokio::select! {
            biased;
            _ = owner.cancelled() => return Err(interrupted("linked candidate was stopped")),
            _ = cancel.cancelled() => return Err(interrupted("linked BuildNew caller cancelled")),
            result = with_deadline(demand.context.deadline, resolution) => result.map_err(|source| EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source)))??,
        };
        demand.validate()?;
        self.recipe.check_staged()?;
        Ok(ResolvedLinkedInvocation {
            recipe: self.recipe,
            peers: self.peers,
            endpoints,
            demand,
            egress: upstream.egress_control(),
        })
    }
}
impl ResolvedLinkedInvocation {
    /// Only after the actual canonical slot's fallible start succeeded. Missing
    /// or retired actual cells are errors; no legacy/local worker is started.
    pub(in crate::adapters::wireguard::lifecycle) fn install(
        self,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        self.demand.validate()?;
        self.recipe.check_staged()?;
        if self.egress.generation() != self.recipe.binding.network_generation.0 {
            return Err(invalid("linked candidate network changed during BuildNew"));
        }
        let devices = (0..self.peers)
            .map(|peer| {
                self.recipe
                    .pending
                    .peer_stack(peer)
                    .and_then(|owner| owner.device())
                    .filter(|device| device.is_usable())
                    .ok_or_else(|| invalid("canonical linked peer has not confirmed start"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let link = Arc::new(inbound::LinkedEndpoint::new(
            self.recipe.identity,
            self.recipe.binding.network_generation.0,
            self.recipe.pending.listen().clone(),
            self.recipe.pending.live(),
            devices,
            self.endpoints,
            vec![None; self.recipe.plan.peer_count()],
            self.recipe.sender,
            self.recipe.packets,
        ));
        self.recipe.pending.install(link.clone())?;
        self.demand.validate()?;
        LinkedEndpointRecipe::check_owner(
            &self.recipe.map,
            &self.recipe.tag,
            &self.recipe.pending,
        )?;
        if self.egress.generation() != self.recipe.binding.network_generation.0 {
            return Err(invalid(
                "linked candidate network changed during installation",
            ));
        }
        Ok(link)
    }
}
fn interrupted(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        message,
    ))
}
