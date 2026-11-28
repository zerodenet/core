pub(super) mod lender;
pub(super) mod lower;
mod ordinary;
mod packet_metadata;
mod pool;
mod production;
use super::super::VlessAdapter;
use crate::component::{
    background::{pool as background_pool, stateless},
    RegistrationError, ResourcePreparation,
};
pub(crate) use packet_metadata::declare_packet_path;
pub(crate) use production::needs_ip_backend;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};

pub(crate) enum Options {
    Reverse(Arc<::vless::reverse::component::Options>),
    Ordinary(
        Arc<stateless::Options<::vless::component::Options>>,
        Arc<crate::component::background::BackgroundNetwork>,
    ),
    Mux(Arc<::vless::component::mux::Options>),
    Mixed(
        Arc<stateless::Options<::vless::component::mux::Options>>,
        Arc<crate::component::background::BackgroundNetwork>,
    ),
    Pooled(Arc<::vless::component::grpc::StreamOptions>),
    Quic(Arc<super::quic::Options>),
    Xhttp(Arc<super::xhttp::Options>),
    Browser(Arc<super::browser::Options>),
}
impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        if let Self::Ordinary(options, _) = self {
            *options = Arc::new((**options).clone().with_node_dns(dependency));
        }
    }
}
pub(super) fn vision(input: &OutboundConfig) -> bool {
    matches!(&input.protocol, OutboundProtocolConfig::Vless { flow: Some(flow), .. }
        if ::vless::validation::is_vision_flow(flow))
}
pub(crate) fn taskless_domain(input: &OutboundConfig) -> bool {
    super::native_input(input)
        && matches!(&input.protocol,OutboundProtocolConfig::Vless{server,..} if zero_core::address::parse_ip_literal(server).is_none())
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Reverse(options) => ::vless::reverse::component::Factory.describe(options),
            Options::Ordinary(options, _) => {
                stateless::Factory(::vless::component::Factory).describe(options)
            }
            Options::Mux(options) => ::vless::component::mux::Factory.describe(options),
            Options::Mixed(options, network) => stateless::Factory(super::material::MuxFactory {
                network: network.clone(),
            })
            .describe(options),
            Options::Pooled(options) => ::vless::component::grpc::StreamFactory.describe(options),
            Options::Quic(options) => super::quic::Factory.describe(options),
            Options::Xhttp(options) => super::xhttp::Factory.describe(options),
            Options::Browser(options) => super::browser::Factory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Reverse(options) => {
                ::vless::reverse::component::Factory.build(options.clone(), context)
            }
            Options::Ordinary(options, network) => stateless::Factory(super::material::Factory {
                network: network.clone(),
            })
            .build(options.clone(), context),
            Options::Mux(options) => {
                ::vless::component::mux::Factory.build_hosted(options.clone(), context, None)
            }
            Options::Mixed(options, network) => stateless::Factory(super::material::MuxFactory {
                network: network.clone(),
            })
            .build(options.clone(), context),
            Options::Pooled(options) => {
                ::vless::component::grpc::StreamFactory.build_hosted(options.clone(), context)
            }
            Options::Quic(options) => super::quic::Factory.build(options.clone(), context),
            Options::Xhttp(options) => super::xhttp::Factory.build(options.clone(), context),
            Options::Browser(options) => super::browser::Factory.build(options.clone(), context),
        }
    }
}
pub(crate) fn grpc_eligible(input: &OutboundConfig) -> bool {
    let mut ordinary = input.clone();
    let OutboundProtocolConfig::Vless {
        mux_concurrency,
        xudp_concurrency,
        grpc: Some(_),
        ..
    } = &mut ordinary.protocol
    else {
        return false;
    };
    let mux = mux_concurrency.take();
    let xudp = xudp_concurrency.take();
    if mux.is_none() && xudp.is_none() && !vision(input) {
        return false;
    }
    // Every transport/security setting remains in the real lower recipe. This
    // probe removes only framing owned by the actual upper MUX driver.
    super::grpc::eligible(&ordinary) || super::preconnect::eligible(&ordinary)
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if matches!(input.protocol, OutboundProtocolConfig::VlessReverse) {
        return true;
    }
    if super::preconnect::eligible(input)
        || super::owned_input(input)
        || super::grpc::eligible(input)
        || grpc_eligible(input)
        || super::quic::eligible(input)
        || super::xhttp::eligible(input)
        || super::browser::eligible(input)
    {
        return true;
    }
    let mut ordinary = input.clone();
    let OutboundProtocolConfig::Vless {
        mux_concurrency,
        xudp_concurrency,
        flow,
        ..
    } = &mut ordinary.protocol
    else {
        return false;
    };
    let mux = mux_concurrency.take();
    let xudp = xudp_concurrency.take();
    if mux.is_none() && xudp.is_none() && !vision(input)
        || flow.as_deref().is_some_and(|flow| {
            flow != "zero-aead-v1" && !::vless::validation::is_vision_flow(flow)
        })
        || flow
            .as_deref()
            .is_some_and(::vless::validation::is_vision_flow)
            && mux.is_some()
    {
        return false;
    }
    super::owned_input(&ordinary)
        || super::preconnect::carrier_eligible(&ordinary)
        || lower::eligible(&ordinary)
}
impl VlessAdapter {
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
        if matches!(input.protocol, OutboundProtocolConfig::VlessReverse) {
            return Ok(Options::Reverse(Arc::new(super::reverse::options(
                self, input, resources,
            )?)));
        }
        if super::preconnect::eligible(input) {
            return self.preconnect_options(input, resources, factory);
        }
        if super::browser::eligible(input) && !vision(input) {
            return self.browser_options(input, resources, factory.network.clone());
        }
        if super::xhttp::eligible(input) && !vision(input) {
            return self.xhttp_options(input, resources, factory.network.clone());
        }
        if super::quic::eligible(input) && !vision(input) {
            return self.quic_options(input, resources, factory.network.clone());
        }
        if super::grpc::eligible(input) && !vision(input) {
            return self.grpc_options(input, resources, &factory);
        }
        let OutboundProtocolConfig::Vless {
            server,
            port,
            mux_concurrency,
            xudp_concurrency,
            ..
        } = &input.protocol
        else {
            unreachable!("owning adapter projection")
        };
        if mux_concurrency.is_none() && xudp_concurrency.is_none() && !vision(input) {
            // Physical MUX carriers and taskless native/borrowed originals have
            // different actual owners. A MUX setting must not alter this role.
            let limit = if stream_only {
                NonZeroUsize::new(
                    resources
                        .candidate()
                        .map(|config| config.runtime.effective_native_stream_max_originals())
                        .unwrap_or(256),
                )
                .filter(|limit| limit.get() <= 4096)
                .ok_or_else(|| {
                    RegistrationError::Options(zero_config::ConfigError::InvalidRuntime(
                        "invalid taskless Stream original limit".into(),
                    ))
                })?
            } else {
                limit
            };
            let options =
                self.ordinary_options(input, resources, factory.network.clone(), limit)?;
            return Ok(Options::Ordinary(
                Arc::new(options),
                factory.network.clone(),
            ));
        }
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning protocol projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: factory.generation,
        };
        let mut prepared = ::vless::component::mux::PreparedMux::from_owned_options_refs(
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
            || matches!(&input.protocol, OutboundProtocolConfig::Vless { tls:Some(tls),.. } if tls.options.ech_config_list.contains("://"))
        {
            let node =
                crate::component::native_dns::prepare_node(resources, factory.network.clone())?;
            prepared = prepared.with_node_dependency(node);
        }
        let (mut prepared, lower, ordinary_grpc) =
            self.mux_lower_options(input, resources, &factory, prepared, limit)?;
        pool::reuse_material(&mut prepared, resources, &input.tag)?;
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
        let metadata_factory = pool::Factory {
            background: factory.clone(),
        };
        prepared
            .remember_cold_material_identity(
                &metadata_factory.describe(&pool_options).static_identity,
            )
            .map_err(|source| zero_config::ConfigError::InvalidOutbound(source.to_string()))?;
        let owner = match lower {
            Some((dependency, source)) => resources.share(
                "vless.mux.actual_pool",
                Arc::new(lender::Factory {
                    background: factory.clone(),
                    dependency,
                    source,
                }),
                pool_options,
            )?,
            None => resources.share(
                "vless.mux.actual_pool",
                Arc::new(metadata_factory),
                pool_options,
            )?,
        };
        ordinary::compose(
            self,
            input,
            prepared.bind(owner),
            ordinary_grpc,
            resources,
            factory,
        )
    }
}
