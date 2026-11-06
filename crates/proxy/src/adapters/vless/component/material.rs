//! Ordinary TLS uses the same configured Slot, File resource and current ECH root.
use super::*;
use crate::component::{
    background::{stateless, BackgroundNetwork},
    RegistrationError, ResourcePreparation,
};
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
pub(super) struct Factory {
    pub(super) network: Arc<BackgroundNetwork>,
}
impl ComponentFactory for Factory {
    type Options = ::vless::component::Options;
    fn describe(&self, options: &Self::Options) -> ComponentSpec {
        ::vless::component::Factory.describe(options)
    }
    fn build(
        &self,
        options: Arc<Self::Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        ::vless::component::Factory.build_with_ech_resolver(
            options,
            context,
            crate::component::background::grpc::tls_resolver(self.network.clone(), instance),
        )
    }
}
impl super::super::VlessAdapter {
    pub(super) fn ordinary_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        limit: NonZeroUsize,
    ) -> Result<stateless::Options<::vless::component::Options>, RegistrationError> {
        let OutboundProtocolConfig::Vless {
            server, port, tls, ..
        } = &input.protocol
        else {
            unreachable!("actual VLESS factory projection")
        };
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("actual VLESS options");
        let max = resources
            .candidate()
            .map(|config| config.runtime.effective_file_read_max_bytes())
            .unwrap_or_else(|| {
                zero_config::RuntimeOptionsConfig::default().effective_file_read_max_bytes()
            });
        let mut options = ::vless::component::Options::from_owned_options_refs(
            input_refs,
            input.dial.to_policy(),
        )
        .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?
        .with_owned_material(
            &self.runtime,
            resources.source_dir(),
            NonZeroUsize::new(max)
                .ok_or_else(|| ConfigError::InvalidRuntime("zero actual File limit".into()))?,
        )
        .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
        if tls.as_ref().is_some_and(|tls| tls.ca_cert_path.is_some())
            && resources.same_source_as_previous()
        {
            if let Some(previous) = resources.previous_named_instance(&input.tag) {
                let inherited = options
                    .reuse_file_material_from(previous)
                    .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                if !inherited {
                    if let Some(identity) = resources.previous_resource_identity(previous.component)
                    {
                        options
                            .reuse_cold_file_material_from(identity)
                            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
                    }
                }
            }
        }
        let mut result = stateless::Options::new(
            options,
            crate::inventory::service::endpoint(server, *port),
            input.dial.to_policy(),
            limit,
        )
        .with_cleanup_network(network.clone());
        if zero_core::address::parse_ip_literal(server).is_none()
            || tls
                .as_ref()
                .is_some_and(|tls| tls.options.ech_config_list.contains("://"))
        {
            let dependency =
                crate::component::native_dns::prepare_node(resources, network.clone())?;
            result = result.with_node_dns(dependency);
        }
        let identity = stateless::Factory(Factory { network })
            .describe(&result)
            .static_identity;
        result
            .inner_options()
            .remember_cold_material_identity(&identity)
            .map_err(|error| ConfigError::InvalidOutbound(error.to_string()))?;
        Ok(result)
    }
}
