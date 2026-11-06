mod pool;
mod production;
use super::super::VmessAdapter;
use crate::component::{
    background::pool as background_pool, RegistrationError, ResourcePreparation,
};
pub(crate) use production::{has_mux, needs_ip_backend};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_config::{OutboundConfig, OutboundProtocolConfig};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};

pub(crate) enum Options {
    Ordinary(Arc<super::grpc::Options>),
    Mux(Arc<::vmess::component::mux::Options>),
}
impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        if let Self::Ordinary(options) = self {
            Arc::make_mut(options).bind_node_dns(dependency);
        }
    }
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Ordinary(options) => super::grpc::Factory.describe(options),
            Options::Mux(options) => ::vmess::component::mux::Factory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Ordinary(options) => super::grpc::Factory.build(options.clone(), context),
            Options::Mux(options) => {
                ::vmess::component::mux::Factory.build(options.clone(), context)
            }
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if super::grpc::owned_eligible(input) {
        return true;
    }
    let mut ordinary = input.clone();
    let OutboundProtocolConfig::Vmess {
        mux_concurrency, ..
    } = &mut ordinary.protocol
    else {
        return false;
    };
    if mux_concurrency.take().is_none() {
        return false;
    }
    if let OutboundProtocolConfig::Vmess { grpc, .. } = &mut ordinary.protocol {
        // Only the ordinary-shape probe omits gRPC. Actual MUX preparation
        // validates and stores its full profile and same global admission.
        *grpc = None;
    }
    super::owned_input(&ordinary)
}
impl VmessAdapter {
    pub(crate) fn owned_mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<background_pool::Factory>,
    ) -> Result<Options, RegistrationError> {
        self.mux_options(
            input,
            resources,
            factory,
            NonZeroUsize::new(1).unwrap(),
            false,
        )
    }
    pub(crate) fn production_mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<background_pool::Factory>,
    ) -> Result<Options, RegistrationError> {
        let limit = resources
            .candidate()
            .map(|c| c.runtime.effective_native_mux_max_carriers())
            .unwrap_or(4);
        let limit = NonZeroUsize::new(limit)
            .filter(|n| n.get() <= 4096)
            .ok_or_else(|| {
                RegistrationError::Options(zero_config::ConfigError::InvalidRuntime(
                    "invalid native MUX carrier limit".into(),
                ))
            })?;
        self.mux_options(input, resources, factory, limit, true)
    }

    fn mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<background_pool::Factory>,
        limit: NonZeroUsize,
        stream_only: bool,
    ) -> Result<Options, RegistrationError> {
        let OutboundProtocolConfig::Vmess {
            server,
            port,
            mux_concurrency,
            ..
        } = &input.protocol
        else {
            unreachable!("owning adapter projection")
        };
        if mux_concurrency.is_none() {
            return self
                .owned_standard_options(input, resources, factory.network.clone())
                .map(|options| Options::Ordinary(Arc::new(options)));
        }
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning protocol projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: factory.generation,
        };
        let mut prepared = ::vmess::component::mux::PreparedMux::from_owned_options_refs(
            input_refs,
            &self.runtime,
            resources
                .candidate()
                .and_then(|candidate| candidate.source_dir()),
            binding,
            limit,
            Duration::from_secs(5),
            NonZeroUsize::new(
                resources
                    .candidate()
                    .map(|config| config.runtime.effective_file_read_max_bytes())
                    .unwrap_or_else(|| {
                        zero_config::RuntimeOptionsConfig::default().effective_file_read_max_bytes()
                    }),
            )
            .ok_or_else(|| {
                zero_config::ConfigError::InvalidRuntime("zero File material bound".into())
            })?,
        )
        .map_err(|source| {
            zero_config::ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        if zero_core::address::parse_ip_literal(server).is_none()
            || matches!(&input.protocol, OutboundProtocolConfig::Vmess { tls:Some(tls),.. } if tls.options.ech_config_list.contains("://"))
        {
            let node =
                crate::component::native_dns::prepare_node(resources, factory.network.clone())?;
            prepared = prepared.with_node_dependency(node);
        }
        if resources.same_source_as_previous() {
            if let Some(dependencies) = resources.previous_named_dependencies(&input.tag) {
                for dependency in dependencies {
                    let inherited =
                        prepared
                            .reuse_file_material_from(*dependency)
                            .map_err(|source| {
                                zero_config::ConfigError::InvalidOutbound(source.to_string())
                            })?;
                    let inherited = inherited
                        || resources
                            .previous_resource_identity(dependency.component)
                            .map(|identity| prepared.reuse_cold_file_material_from(identity))
                            .transpose()
                            .map_err(|source| {
                                zero_config::ConfigError::InvalidOutbound(source.to_string())
                            })?
                            .unwrap_or(false);
                    if inherited {
                        break;
                    }
                }
            }
        }
        let factory = Arc::new(pool::Factory {
            background: factory,
        });
        let pool_options = Arc::new(pool::Options {
            pool: Arc::new(
                (*prepared.owned_pool_options())
                    .clone()
                    .with_scoped_preparation(),
            ),
            tag: input.tag.clone(),
            wire_endpoint: zero_core::request::Destination::new(
                zero_core::Address::Domain(server.clone()),
                *port,
            ),
        });
        prepared
            .remember_cold_material_identity(&factory.describe(&pool_options).static_identity)
            .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.to_string()))?;
        let owner = resources.share("vmess.mux.actual_pool", factory, pool_options)?;
        let options = prepared.bind(owner);
        Ok(Options::Mux(Arc::new(if stream_only {
            options.stream_only()
        } else {
            options
        })))
    }
}
