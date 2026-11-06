mod production;
use super::super::VlessAdapter;
use crate::component::{
    background::{pool, stateless},
    RegistrationError, ResourcePreparation,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_config::{OutboundConfig, OutboundProtocolConfig};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};

pub(crate) enum Options {
    Ordinary(
        Arc<stateless::Options<::vless::component::Options>>,
        Arc<crate::component::background::BackgroundNetwork>,
    ),
    Mux(Arc<::vless::component::mux::Options>),
    Grpc(Arc<::vless::component::grpc::StreamOptions>),
}
impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        if let Self::Ordinary(options, _) = self {
            *options = Arc::new((**options).clone().with_node_dns(dependency));
        }
    }
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
            Options::Ordinary(options, _) => {
                stateless::Factory(::vless::component::Factory).describe(options)
            }
            Options::Mux(options) => ::vless::component::mux::Factory.describe(options),
            Options::Grpc(options) => ::vless::component::grpc::StreamFactory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Ordinary(options, network) => stateless::Factory(super::material::Factory {
                network: network.clone(),
            })
            .build(options.clone(), context),
            Options::Mux(options) => {
                ::vless::component::mux::Factory.build(options.clone(), context)
            }
            Options::Grpc(options) => {
                ::vless::component::grpc::StreamFactory.build(options.clone(), context)
            }
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if super::owned_input(input) || super::grpc::eligible(input) {
        return true;
    }
    let mut ordinary = input.clone();
    let OutboundProtocolConfig::Vless {
        mux_concurrency,
        xudp_concurrency,
        server,
        flow,
        ..
    } = &mut ordinary.protocol
    else {
        return false;
    };
    let mux = mux_concurrency.take();
    if xudp_concurrency.take().is_some_and(|x| Some(x) != mux) {
        return false;
    }
    if mux.is_none() || zero_core::address::parse_ip_literal(server).is_none() || flow.is_some() {
        return false;
    }
    super::native_input(&ordinary)
}
impl VlessAdapter {
    pub(crate) fn owned_mux_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<pool::Factory>,
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
        factory: Arc<pool::Factory>,
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
        factory: Arc<pool::Factory>,
        limit: NonZeroUsize,
        stream_only: bool,
    ) -> Result<Options, RegistrationError> {
        if super::grpc::eligible(input) {
            return self.grpc_options(input, resources, &factory);
        }
        let OutboundProtocolConfig::Vless {
            server,
            port,
            mux_concurrency,
            ..
        } = &input.protocol
        else {
            unreachable!("owning adapter projection")
        };
        if mux_concurrency.is_none() {
            // Physical MUX carriers and taskless native/borrowed originals have
            // different actual owners. A MUX setting must not alter this role.
            let limit = if stream_only {
                NonZeroUsize::new(
                    resources
                        .candidate()
                        .map(|config| config.runtime.effective_native_stream_max_originals())
                        .unwrap_or(4),
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
        let mut prepared = ::vless::component::mux::PreparedMux::from_options_refs(
            input_refs,
            &self.runtime,
            resources
                .candidate()
                .and_then(|candidate| candidate.source_dir()),
            binding,
            limit,
            Duration::from_secs(5),
        )
        .map_err(|source| {
            zero_config::ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        if zero_core::address::parse_ip_literal(server).is_none() {
            let node =
                crate::component::native_dns::prepare_node(resources, factory.network.clone())?;
            prepared = prepared.with_node_dependency(node);
        }
        let owner = resources.share(
            "vless.mux.actual_pool",
            factory,
            Arc::new(pool::Options {
                pool: prepared.pool_options().clone(),
                tag: input.tag.clone(),
                wire_endpoint: zero_core::request::Destination::new(
                    zero_core::Address::Domain(server.clone()),
                    *port,
                ),
            }),
        )?;
        let options = prepared.bind(owner);
        Ok(Options::Mux(Arc::new(if stream_only {
            options.stream_only()
        } else {
            options
        })))
    }
}
