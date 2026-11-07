//! Install only the exact current Source grant into this cold graph.
use super::*;
impl WireguardAdapter {
    /// Called only alongside actual Source-origin projection by the ordinary
    /// host. The exact pending graph gets its same Source allocation view;
    /// this never reconstructs a memory Root from a tag or component identity.
    pub(crate) fn install_source_memory(
        &self,
        config: &zero_config::RuntimeConfig,
        tag: &str,
        memory: zero_tun::packet_memory::PacketMemoryView,
    ) -> Result<(), EngineError> {
        let linked = config.outbounds.iter().any(|outbound| {
            matches!(&outbound.protocol,
            OutboundProtocolConfig::Wireguard {inbound_tag:Some(source),outer_udp_proxy:None,..}
                if source==tag)
        });
        if !linked {
            return Ok(());
        }
        if !self
            .packet_return_totals
            .get()
            .is_some_and(|root| root.owns_view(&memory))
        {
            return Err(invalid(
                "Source memory belongs to another actual Kernel Root",
            ));
        }
        let pending = PendingEndpoints::staged(&self.pending_endpoints, config)
            .and_then(|candidate| candidate.get(tag).cloned())
            .ok_or_else(|| invalid("Source memory has no same actual linked candidate"))?;
        let graph = pending
            .actual_graph()
            .ok_or_else(|| invalid("actual linked memory factory owner was released"))?;
        graph.install_source_memory(memory)
    }
}

impl WireguardAdapter {
    pub(in crate::adapters::wireguard) fn source_outer_graph_dependency(
        &self,
        config: &zero_config::RuntimeConfig,
        source: &str,
    ) -> Result<Option<ComponentId>, EngineError> {
        let candidate = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        if !candidate.candidate_matches(config) {
            return Err(invalid("Source graph belongs to another actual candidate"));
        }
        let Some(graph) = candidate
            .get(source)
            .and_then(|pending| pending.actual_graph())
        else {
            return Ok(None);
        };
        if !graph.has_outer_path() {
            return Ok(None);
        }
        graph
            .resource_id()
            .map(Some)
            .ok_or_else(|| invalid("actual outer graph has no same-pass component ID"))
    }
}

impl WireguardAdapter {
    pub(crate) fn prepared_outer_materials(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<
        HashMap<String, Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>>,
        EngineError,
    > {
        let candidate = self
            .pending_endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(candidate) = candidate else {
            return Ok(HashMap::new());
        };
        if !candidate.candidate_matches(config) {
            return Err(invalid(
                "formatter materials belong to another actual candidate",
            ));
        }
        let mut values = HashMap::new();
        for pending in candidate.values() {
            let graph = pending
                .actual_graph()
                .ok_or_else(|| invalid("actual formatter graph owner was released"))?;
            if let Some((tag, formatters)) = graph.outer_materials() {
                match values.entry(tag.to_owned()) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(formatters.clone());
                    }
                    std::collections::hash_map::Entry::Occupied(entry) => {
                        if !Arc::ptr_eq(entry.get(), formatters) {
                            return Err(invalid(
                                "same cold outer path has different actual association families",
                            ));
                        }
                    }
                }
            }
        }
        Ok(values)
    }
}
