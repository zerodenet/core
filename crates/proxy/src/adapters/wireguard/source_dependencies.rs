//! Actual cold consumer references for the existing TEMP outer packet path.
//! This does not open that path, select another group, or prove native close.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use zero_core::component::ComponentId;

pub(crate) type SourceComponentDependencies = BTreeMap<String, ([u8; 32], BTreeSet<ComponentId>)>;
impl WireguardAdapter {
    pub(crate) fn source_component_dependencies(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        resources: &crate::component::ResourcePreparation<'_>,
    ) -> Result<SourceComponentDependencies, EngineError> {
        if !resources
            .candidate()
            .is_some_and(|candidate| std::ptr::eq(candidate, config.as_ref()))
        {
            return Err(invalid(
                "Source dependencies differ from this actual candidate snapshot",
            ));
        }
        let mut sources: SourceComponentDependencies = BTreeMap::new();
        for outbound in &config.outbounds {
            let OutboundProtocolConfig::Wireguard {
                inbound_tag: Some(source),
                outer_udp_proxy: Some(outer),
                ..
            } = &outbound.protocol
            else {
                continue;
            };
            let input = config
                .inbounds
                .iter()
                .find(|input| input.tag == *source)
                .ok_or_else(|| invalid("actual outer consumer Source is absent"))?;
            let digest = crate::runtime::inbound_owners::source_input_identity(input)?;
            let actual = outer_component_dependencies(config, outer, resources)?;
            let entry = sources
                .entry(source.clone())
                .or_insert_with(|| (digest, BTreeSet::new()));
            if entry.0 != digest {
                return Err(invalid("one actual Source has different candidate input"));
            }
            entry.1.extend(actual);
        }
        Ok(sources)
    }
}
/// The existing packet-path preparer supports named leaves and flat Relay
/// groups only. Capture all of those actual members, never only today's first
/// selected path. Protocol dependencies follow each same-pass actual node ID.
pub(in crate::adapters::wireguard) fn outer_component_dependencies(
    config: &zero_config::RuntimeConfig,
    outer: &str,
    resources: &crate::component::ResourcePreparation<'_>,
) -> Result<BTreeSet<ComponentId>, EngineError> {
    let names: Vec<&str> = if config
        .outbounds
        .iter()
        .any(|outbound| outbound.tag == outer)
    {
        vec![outer]
    } else {
        let group = config
            .outbound_groups
            .iter()
            .find(|group| group.tag == outer)
            .ok_or_else(|| invalid("actual outer packet path is absent"))?;
        let zero_config::OutboundGroupKind::Relay { proxies } = &group.group else {
            return Err(invalid(
                "existing outer packet path requires a flat Relay group",
            ));
        };
        if proxies.is_empty() {
            return Err(invalid("existing outer Relay has no actual members"));
        }
        proxies.iter().map(String::as_str).collect()
    };
    names
        .into_iter()
        .map(|name| {
            if !config.outbounds.iter().any(|outbound| outbound.tag == name) {
                return Err(invalid(
                    "existing outer Relay member is not a named outbound",
                ));
            }
            resources.named_component(name).ok_or_else(|| {
                invalid("outer consumer has no actual component ID in this cold pass")
            })
        })
        .collect()
}
