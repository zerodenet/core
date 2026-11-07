//! Resolve only this actual cold candidate before deferred device startup.
use super::*;
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{CarrierContext, UpstreamBinding},
    CancellationToken,
};
impl WireguardAdapter {
    pub(crate) async fn resolve_component_resources(
        &self,
        config: &zero_config::RuntimeConfig,
        upstream: &crate::protocol_registry::UpstreamConnectServices,
        traffic: &zero_engine::Engine,
        deadline: tokio::time::Instant,
        cancel: CancellationToken,
    ) -> Result<(), EngineError> {
        let Some(candidate) = PendingEndpoints::staged(&self.pending_endpoints, config) else {
            return Ok(());
        };
        let roots = candidate
            .standalone
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(tag, candidate)| (tag.clone(), candidate.root.clone()))
            .collect::<Vec<_>>();
        for (tag, root) in roots {
            let outbound = config
                .outbounds
                .iter()
                .find(|outbound| outbound.tag == tag)
                .ok_or_else(|| invalid("actual standalone root has no candidate outbound"))?;
            let prepared = config
                .endpoint_bindings()
                .iter()
                .find(|binding| binding.canonical && binding.outbound_tags.contains(&outbound.tag))
                .and_then(|binding| {
                    traffic.prepare_endpoint_traffic(binding, &root.plan.peer_ids())
                });
            let meters = (0..root.peers.len())
                .map(|peer| {
                    prepared
                        .as_ref()
                        .map(|prepared| {
                            crate::runtime::raw_ip::RawIpTraffic::new(
                                prepared.endpoint().clone(),
                                prepared.peers().get(peer).cloned(),
                            )
                        })
                        .unwrap_or_default()
                        .with_global(traffic.traffic_meter(&zero_api::TrafficScope::Global))
                })
                .collect();
            root.resolve(upstream, meters, deadline, cancel.clone())
                .await?;
            if let Some(prepared) = prepared {
                candidate
                    .traffic
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entry(tag)
                    .or_insert(prepared);
            }
        }
        let mut invocations = candidate.invocations.lock().await;
        for (tag, invocation) in invocations.iter_mut() {
            let outbound = config.outbounds.iter().find(|outbound|matches!(&outbound.protocol, OutboundProtocolConfig::Wireguard {inbound_tag:Some(inbound),outer_udp_proxy:None,..} if inbound == tag)).ok_or_else(||invalid("actual linked invocation has no candidate outbound"))?;
            let pending = candidate
                .get(tag)
                .ok_or_else(|| invalid("actual linked peer originals are missing"))?;
            let graph = pending
                .actual_graph()
                .ok_or_else(|| invalid("actual linked factory owner was released"))?;
            let plan = WireguardRawIpPlan::from_protocol(&outbound.protocol)?;
            let prepared = config
                .endpoint_bindings()
                .iter()
                .find(|binding| binding.canonical && binding.outbound_tags.contains(&outbound.tag))
                .and_then(|binding| traffic.prepare_endpoint_traffic(binding, &plan.peer_ids()));
            let traffic = (0..graph.peer_count())
                .map(|peer| {
                    prepared
                        .as_ref()
                        .map(|prepared| {
                            crate::runtime::raw_ip::RawIpTraffic::new(
                                prepared.endpoint().clone(),
                                prepared.peers().get(peer).cloned(),
                            )
                        })
                        .unwrap_or_default()
                        .with_global(traffic.traffic_meter(&zero_api::TrafficScope::Global))
                })
                .collect();
            invocation
                .resolve(
                    upstream,
                    crate::runtime::udp_dispatch::packet_owner::PacketCarrierDemand {
                        identity: RequestIdentity::Service(
                            Arc::new(zero_core::request::ServiceRequestFacts {
                                origin: zero_core::request::ServiceOrigin::Runtime,
                                purpose: zero_core::request::ServicePurpose::BackgroundControl,
                            })
                            .into(),
                        ),
                        context: CarrierContext {
                            session: None,
                            deadline: Some(deadline),
                            cancel: cancel.clone(),
                        },
                        binding: UpstreamBinding {
                            prefix: Vec::new(),
                            dial: outbound.dial.to_policy(),
                            network_generation: NetworkGeneration(upstream.egress_generation()),
                        },
                    },
                )
                .await?;
            graph.apply_traffic(traffic)?;
            if let Some(prepared) = prepared {
                candidate
                    .traffic
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(tag.clone())
                    .or_insert(prepared);
            }
        }
        Ok(())
    }
}
