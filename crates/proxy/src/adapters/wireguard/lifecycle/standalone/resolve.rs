//! Actual Node DNS resolves this immutable candidate before deferred startup.
use super::*;
use crate::protocol_registry::UpstreamConnectServices;
use zero_platform_tokio::contract::CancellationToken;
impl StandaloneStackOptions {
    pub(in crate::adapters::wireguard::lifecycle) async fn resolve(
        &self,
        upstream: &UpstreamConnectServices,
        traffic: Vec<crate::runtime::raw_ip::RawIpTraffic>,
        deadline: tokio::time::Instant,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        if upstream.egress_generation() != self.binding.network_generation.0
            || cancel.is_cancelled()
        {
            return Err(invalid("standalone actual network/candidate was revoked"));
        }
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.stopped {
                return Err(invalid("standalone original graph is retired"));
            }
            if state.endpoints.is_some() {
                return Ok(());
            }
            if state.attempted || traffic.len() != state.stacks.len() {
                return Err(invalid(
                    "standalone actual stack/traffic declaration differs",
                ));
            }
            for (options, traffic) in state.stacks.iter_mut().zip(traffic) {
                options.traffic = traffic;
            }
        }
        let work = async {
            let mut endpoints = Vec::with_capacity(self.peers.len());
            for index in 0..self.peers.len() {
                let (host, port) = self
                    .plan
                    .peer_endpoint(index)
                    .ok_or_else(|| invalid("standalone peer has no actual endpoint"))?;
                let address = match host.parse::<IpAddr>() {
                    Ok(IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
                    Ok(IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
                    Err(_) => Address::Domain(host.to_owned()),
                };
                endpoints.push(
                    upstream
                        .resolve_node_address(&address, port, "WireGuard peer endpoint")
                        .await?,
                );
            }
            Ok::<_, EngineError>(endpoints)
        };
        let endpoints = tokio::select! { biased;
            _ = cancel.cancelled() => return Err(invalid("standalone candidate resolution was cancelled")),
            result = tokio::time::timeout_at(deadline, work) => result.map_err(std::io::Error::other)??,
        };
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.stopped
            || state.attempted
            || upstream.egress_generation() != self.binding.network_generation.0
        {
            return Err(invalid(
                "standalone original graph changed while resolving its endpoint",
            ));
        }
        state.endpoints = Some(endpoints);
        Ok(())
    }
}
