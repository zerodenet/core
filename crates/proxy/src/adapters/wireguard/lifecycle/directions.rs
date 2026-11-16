//! Direction policy does not replace the retained shared endpoint's resources.
use super::*;

impl WireguardAdapter {
    pub(super) fn retained_peer_count(
        &self,
        outbound: &OutboundConfig,
        inbounds: &[InboundConfig],
        plan: &WireguardRawIpPlan,
        identity: [u8; 32],
        generation: u64,
        disabled: bool,
    ) -> Result<usize, EngineError> {
        if !disabled {
            return Ok(plan.peer_count());
        }
        // A disabled direction reuses only the same actual host-owned graph.
        // No newly disabled endpoint acquires client stacks.
        let OutboundProtocolConfig::Wireguard {
            inbound_tag: Some(tag),
            ..
        } = &outbound.protocol
        else {
            return Ok(0);
        };
        let Some(inbound) = inbounds.iter().find(|inbound| &inbound.tag == tag) else {
            return Ok(0);
        };
        let inbound_identity = super::super::inbound_protocol_identity(&inbound.protocol)
            .ok_or_else(|| invalid("cannot identify retained WireGuard inbound"))?;
        let link = self
            .linked_endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(tag)
            .filter(|link| {
                link.listen == inbound.listen
                    && link.live.listener_active()
                    && link.live.matches_identity(inbound_identity)
            })
            .cloned();
        let Some(link) = link else { return Ok(0) };
        if link.owned_resources().is_none() {
            return Err(invalid(
                "retained linked endpoint has no actual resource owner",
            ));
        }
        let devices = {
            let state = link.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.identity != identity || state.generation != generation {
                return Ok(0);
            }
            state.devices.clone()
        };
        if devices.len() != plan.peer_count() {
            return Ok(0);
        }
        // Reuse only the same actual pool entries, not historical link views.
        let retained = devices.iter().enumerate().all(|(peer, device)| {
            self.pool
                .reusable_cell(&outbound.tag, peer, identity, generation)
                .and_then(|cell| cell.get().cloned())
                .is_some_and(|current| Arc::ptr_eq(&current, device))
        });
        Ok(if retained { devices.len() } else { 0 })
    }
}
