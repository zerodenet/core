//! Same installed Node owner and current resolver for all ordinary TLS material.
use super::*;
use crate::component::{background::BackgroundNetwork, RegistrationError, ResourcePreparation};
pub(super) struct Factory {
    pub(super) resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
}
impl ComponentFactory for Factory {
    type Options = ::vmess::component::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        ::vmess::component::Factory.describe(options)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match &self.resolver {
            Some(resolver) => ::vmess::component::Factory.build_with_ech_resolver(
                options,
                context,
                resolver.clone(),
            ),
            None => ::vmess::component::Factory.build(options, context),
        }
    }
}
impl super::super::super::VmessAdapter {
    pub(crate) fn owned_standard_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
    ) -> Result<Options, RegistrationError> {
        let OutboundProtocolConfig::Vmess {
            server,
            port,
            tls,
            grpc,
            ..
        } = &input.protocol
        else {
            unreachable!("owning VMess projection")
        };
        let runtime = resources
            .candidate()
            .map(|candidate| candidate.runtime.clone())
            .unwrap_or_default();
        let limit = NonZeroUsize::new(runtime.effective_native_stream_max_originals())
            .filter(|n| n.get() <= 4096)
            .ok_or_else(|| {
                ConfigError::InvalidRuntime("invalid native Stream original limit".into())
            })?;
        let max = NonZeroUsize::new(runtime.effective_file_read_max_bytes())
            .ok_or_else(|| ConfigError::InvalidRuntime("zero File reader limit".into()))?;
        let refs =
            super::super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("actual VMess options");
        let previous = resources
            .same_source_as_previous()
            .then(|| resources.previous_named_instance(&input.tag))
            .flatten();
        let mut selected = if grpc.is_none() {
            let mut base = ::vmess::component::Options::from_owned_options_refs_at(
                refs,
                input.dial.to_policy(),
                resources.source_dir(),
                Some(max),
            )
            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?
            .with_owned_material(&self.runtime, resources.source_dir(), max)
            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
            if let Some(previous) = previous {
                let inherited = base
                    .reuse_file_material_from(previous)
                    .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                if !inherited {
                    if let Some(identity) = resources.previous_resource_identity(previous.component)
                    {
                        base.reuse_cold_file_material_from(identity)
                            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                    }
                }
            }
            Options::Ordinary(Arc::new(
                stateless::Options::new(
                    base,
                    crate::inventory::service::endpoint(server, *port),
                    input.dial.to_policy(),
                    limit,
                )
                .with_cleanup_network(network.clone()),
            ))
        } else {
            let cap = NonZeroUsize::new(runtime.effective_vmess_grpc_max_originals())
                .filter(|n| n.get() <= 4096)
                .ok_or_else(|| {
                    ConfigError::InvalidRuntime("invalid VMess original limit".into())
                })?;
            let mut base =
                ::vmess::component::grpc::Options::from_owned_node_options_refs_with_streams_at(
                    refs,
                    input.dial.to_policy(),
                    resources.source_dir(),
                    Some(max),
                    cap,
                    Duration::from_secs(5),
                    &self.runtime,
                    streams_selected(input, resources.candidate(), self.native_node_dns),
                )
                .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
            if let Some(previous) = previous {
                let inherited = base
                    .reuse_file_material_from(previous)
                    .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                if !inherited {
                    if let Some(identity) = resources.previous_resource_identity(previous.component)
                    {
                        base.reuse_cold_file_material_from(identity)
                            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                    }
                }
            }
            Options::Grpc(Arc::new(base))
        };
        if zero_core::address::parse_ip_literal(server).is_none()
            || tls
                .as_ref()
                .is_some_and(|tls| tls.options.ech_config_list.contains("://"))
        {
            let dns = crate::component::native_dns::prepare_node(resources, network.clone())?;
            selected.bind_node_dns(dns);
        }
        let identity = Factory.describe(&selected).static_identity;
        match &selected {
            Options::Ordinary(options) => options
                .inner_options()
                .remember_cold_material_identity(&identity),
            Options::Grpc(options) => options.remember_cold_material_identity(&identity),
            Options::Owned(..) => unreachable!("owning cold material projection"),
        }
        .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
        Ok(Options::Owned(Arc::new(selected), network))
    }
}
