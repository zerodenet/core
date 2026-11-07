//! Actual currently published stack owners, never the new pending candidate.
use super::*;
use zero_config::RuntimeConfig;
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
impl WireguardAdapter {
    pub(crate) fn actual_packet_flow_sources(
        &self,
        previous: &RuntimeConfig,
    ) -> Result<Vec<(Arc<SharedRawIpDevice>, CarrierOrigin)>, EngineError> {
        let links = self
            .linked_endpoints
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let roots = {
            let profiles = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            profiles
                .iter()
                .map(|(tag, profile)| (tag.clone(), profile.graph.clone()))
                .collect::<HashMap<_, _>>()
        };
        let admission = zero_engine::EndpointAdmission::from_config(previous);
        let mut actual = Vec::new();
        for outbound in previous
            .outbounds
            .iter()
            .filter(|outbound| admission.device_enabled(&outbound.tag))
        {
            let OutboundProtocolConfig::Wireguard {
                inbound_tag,
                outer_udp_proxy,
                ..
            } = &outbound.protocol
            else {
                continue;
            };
            if outer_udp_proxy.is_some() {
                return Err(invalid(
                    "TEMP logical Flow original is not yet installed in the standard source graph",
                ));
            }
            let sources = if let Some(tag) = inbound_tag {
                let link = links
                    .get(tag)
                    .ok_or_else(|| invalid("current linked Source original is unavailable"))?;
                let sources = link
                    .owned_resources()
                    .ok_or_else(|| invalid("current linked Source lacks actual peer ownership"))?
                    .actual_flow_sources()?;
                let devices = link
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .devices
                    .clone();
                if devices.len() != sources.len()
                    || devices
                        .iter()
                        .zip(&sources)
                        .any(|(device, (actual, _))| !Arc::ptr_eq(device, actual))
                {
                    return Err(invalid(
                        "current linked Source and canonical original stacks differ",
                    ));
                }
                sources
            } else {
                roots
                    .get(&outbound.tag)
                    .and_then(|root| root.as_ref())
                    .and_then(std::sync::Weak::upgrade)
                    .ok_or_else(|| invalid("current standalone original graph is unavailable"))?
                    .actual_flow_sources()?
            };
            for (device, origin) in sources {
                if let Some((_, previous)) = actual
                    .iter()
                    .find(|(previous, _)| Arc::ptr_eq(previous, &device))
                {
                    if !origin.same_original(previous) {
                        return Err(invalid("same stack has different actual source originals"));
                    }
                } else {
                    actual.push((device, origin));
                }
            }
        }
        Ok(actual)
    }
}
