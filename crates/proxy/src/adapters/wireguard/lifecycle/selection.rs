use super::*;
use sha2::{Digest, Sha256};
pub(super) type Selection<'a> = (
    &'a OutboundConfig,
    [u8; 32],
    [u8; 32],
    Arc<WireguardRawIpPlan>,
);
impl WireguardAdapter {
    pub(super) fn select_devices<'a>(
        &self,
        outbounds: &[&'a OutboundConfig],
        context: &OutboundDevicePreparationContext,
    ) -> Result<Vec<Selection<'a>>, EngineError> {
        let mut selected = Vec::new();
        for outbound in outbounds {
            if !matches!(outbound.protocol, OutboundProtocolConfig::Wireguard { .. }) {
                continue;
            }
            let identity = protocol_identity(&outbound.protocol)
                .ok_or_else(|| invalid("cannot identify WireGuard outbound"))?;
            let outer_udp_proxy = match &outbound.protocol {
                OutboundProtocolConfig::Wireguard {
                    outer_udp_proxy, ..
                } => outer_udp_proxy.as_deref(),
                _ => None,
            };
            let device_identity = if let Some(proxy_tag) = outer_udp_proxy {
                let proxy_identity = context.packet_paths.get(proxy_tag).map(|path| &path.identity).ok_or_else(|| {
                    invalid(format!(
                        "WireGuard outer_udp_proxy `{proxy_tag}` has no persistent bidirectional UDP packet path"
                    ))
                })?;
                let mut hash = Sha256::new();
                hash.update(identity);
                hash.update(proxy_identity);
                hash.finalize().into()
            } else {
                identity
            };
            let device_identity = context.stack_options.identity(device_identity);
            let plan = self
                .profiles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&outbound.tag)
                .filter(|cached| cached.identity == identity)
                .map(|cached| cached.plan.clone())
                .map(Ok)
                .unwrap_or_else(|| {
                    WireguardRawIpPlan::from_protocol(&outbound.protocol).map(Arc::new)
                })?;
            selected.push((*outbound, identity, device_identity, plan));
        }
        Ok(selected)
    }
}
