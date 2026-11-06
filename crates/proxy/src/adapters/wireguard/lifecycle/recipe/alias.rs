//! A primary outbound aliases its already prepared canonical peer graph.
use super::*;
use zero_platform_tokio::contract::{
    component::{
        BuildContext, ComponentFactory, ComponentInstance, ComponentRoles, ComponentSpec,
        ResourceOwner, RoleKind,
    },
    lifecycle::{erase_lifecycle, BuildError, CloseReport, ResourceLifecycle},
    ChannelError, Impact, Reason, Stage,
};
pub(crate) struct LinkedOutboundFactory;
pub(crate) struct LinkedOutboundOptions {
    graph: Arc<component::LinkedStackOptions>,
    spec: ComponentSpec,
    dependency: ComponentId,
}
impl WireguardAdapter {
    pub(crate) fn linked_outbound_options(
        &self,
        outbound: &OutboundConfig,
        resources: &mut crate::component::ResourcePreparation<'_>,
    ) -> Result<LinkedOutboundOptions, ChannelError> {
        let OutboundProtocolConfig::Wireguard {
            inbound_tag: Some(tag),
            outer_udp_proxy: None,
            ..
        } = &outbound.protocol
        else {
            return Err(error(Reason::InvalidConfig));
        };
        let candidate = self
            .pending_endpoints
            .lock()
            .unwrap()
            .as_ref()
            .cloned()
            .ok_or_else(|| error(Reason::StalePlan))?;
        if !resources
            .candidate()
            .is_some_and(|config| candidate.candidate_matches(config))
        {
            return Err(error(Reason::StalePlan));
        }
        let graph = candidate
            .get(tag)
            .and_then(|pending| pending.actual_graph())
            .ok_or_else(|| error(Reason::StalePlan))?;
        let dependency = *graph
            .resource
            .get()
            .ok_or_else(|| error(Reason::StalePlan))?;
        if !resources.resources().any(|entry| {
            entry.component() == dependency
                && entry.recipe().spec().static_identity == graph.spec.static_identity
        }) {
            return Err(error(Reason::StalePlan));
        }
        use sha2::{Digest, Sha256};
        let mut identity = Sha256::new();
        identity.update(b"zero.wireguard.linked.alias.v1");
        identity.update(graph.spec.static_identity.as_ref());
        for encoded in [
            serde_json::to_vec(&outbound.tag),
            serde_json::to_vec(&outbound.protocol),
            serde_json::to_vec(&outbound.dial),
            serde_json::to_vec(&outbound.udp),
        ] {
            let encoded =
                encoded.map_err(|source| error(Reason::InvalidConfig).with_source(source))?;
            identity.update((encoded.len() as u64).to_le_bytes());
            identity.update(encoded);
        }
        Ok(LinkedOutboundOptions {
            graph,
            dependency,
            spec: ComponentSpec {
                owner: ResourceOwner::Component,
                roles: vec![RoleKind::Packet, RoleKind::Stream, RoleKind::Datagram],
                dependencies: vec![dependency],
                static_identity: identity.finalize().to_vec().into(),
                resources: vec![],
                task_limit: None,
            },
        })
    }
}
impl ComponentFactory for LinkedOutboundFactory {
    type Options = LinkedOutboundOptions;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        options.spec.clone()
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if context.dependencies.len() != 1 || context.dependency(options.dependency).is_none() {
            return Err(error(Reason::StalePlan));
        }
        let carrier = options
            .graph
            .carrier
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| error(Reason::Revoked))?;
        Ok(ComponentInstance {
            roles: ComponentRoles {
                stream: Some(carrier.clone()),
                datagram: Some(carrier.clone()),
                packet: Some(carrier),
                ..Default::default()
            },
            lifecycle: erase_lifecycle(Alias),
        })
    }
}
struct Alias;
impl ResourceLifecycle for Alias {
    async fn start(&mut self, _: tokio::time::Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: zero_platform_tokio::contract::CloseReason) {}
    async fn shutdown(&mut self, _: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        Ok(CloseReport::default())
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}

impl WireguardAdapter {
    /// Exact staged graph IDs for host Source-origin projection. The source
    /// remains the original inbound owner; this only exposes its declared
    /// canonical consumer resource in the same cold preparation pass.
    pub(crate) fn packet_source_resources(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<Vec<(String, ComponentId)>, EngineError> {
        let current = self.pending_endpoints.lock().unwrap().clone();
        let Some(candidate) = current else {
            return Ok(Vec::new());
        };
        if !candidate.candidate_matches(config) {
            return Err(invalid(
                "packet source mapping belongs to another candidate",
            ));
        }
        candidate
            .iter()
            .filter(|(tag, _)| config.outbounds.iter().any(|outbound| matches!(
                &outbound.protocol,
                OutboundProtocolConfig::Wireguard { inbound_tag:Some(inbound),outer_udp_proxy:None,.. } if inbound.as_str()==tag.as_str()
            )))
            .map(|(tag, pending)| {
                let graph = pending
                    .actual_graph()
                    .ok_or_else(|| invalid("packet source graph actual owner was released"))?;
                let resource =
                    graph.resource.get().copied().ok_or_else(|| {
                        invalid("packet source graph is not in this host cold pass")
                    })?;
                Ok((tag.clone(), resource))
            })
            .collect()
    }
}
