mod production;
use super::super::TrojanAdapter;
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
    Ordinary(Arc<stateless::Options<::trojan::component::Options>>),
    Mux(Arc<::trojan::component::mux::Options>),
}
impl Options {
    pub(crate) fn bind_node_dns(&mut self, dependency: zero_core::component::ComponentId) {
        if let Self::Ordinary(options) = self {
            *options = Arc::new((**options).clone().with_node_dns(dependency));
        }
    }
}
pub(crate) fn taskless_domain(input: &OutboundConfig) -> bool {
    super::native_input(input)
        && matches!(&input.protocol,OutboundProtocolConfig::Trojan{server,..} if zero_core::address::parse_ip_literal(server).is_none())
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Ordinary(options) => {
                stateless::Factory(::trojan::component::Factory).describe(options)
            }
            Options::Mux(options) => ::trojan::component::mux::Factory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Ordinary(options) => {
                stateless::Factory(::trojan::component::Factory).build(options.clone(), context)
            }
            Options::Mux(options) => {
                ::trojan::component::mux::Factory.build(options.clone(), context)
            }
        }
    }
}
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    if super::native_input(input) {
        return true;
    }
    let mut ordinary = input.clone();
    let OutboundProtocolConfig::Trojan {
        mux_concurrency,
        server,
        ..
    } = &mut ordinary.protocol
    else {
        return false;
    };
    if mux_concurrency.take().is_none() || zero_core::address::parse_ip_literal(server).is_none() {
        return false;
    }
    super::native_input(&ordinary)
}
impl TrojanAdapter {
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
        let OutboundProtocolConfig::Trojan {
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
            return Ok(Options::Ordinary(Arc::new(
                stateless::Options::new(
                    super::component_options(input)?,
                    zero_core::request::Destination::new(
                        match zero_core::address::parse_ip_literal(server) {
                            Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                            Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                            None => zero_core::Address::Domain(server.clone()),
                        },
                        *port,
                    ),
                    input.dial.to_policy(),
                    limit,
                )
                .with_cleanup_network(factory.network.clone()),
            )));
        }
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning protocol projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: factory.generation,
        };
        let mut prepared = ::trojan::component::mux::PreparedMux::from_options_refs(
            input_refs,
            &self.mux_pool,
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
            "trojan.mux.actual_pool",
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
