use super::super::TrojanAdapter;
use crate::component::{background::pool, RegistrationError, ResourcePreparation};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_config::{OutboundConfig, OutboundProtocolConfig};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};

pub(crate) enum Options {
    Ordinary(Arc<::trojan::component::Options>),
    Mux(Arc<::trojan::component::mux::StreamOptions>),
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Ordinary(options) => ::trojan::component::Factory.describe(options),
            Options::Mux(options) => ::trojan::component::mux::StreamFactory.describe(options),
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Ordinary(options) => {
                ::trojan::component::Factory.build(options.clone(), context)
            }
            Options::Mux(options) => {
                ::trojan::component::mux::StreamFactory.build(options.clone(), context)
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
            return Ok(Options::Ordinary(Arc::new(super::component_options(
                input,
            )?)));
        }
        let input_refs =
            super::super::outbound_options(&input.tag, (server, *port), &input.protocol)
                .expect("owning protocol projection");
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: input.dial.to_policy(),
            network_generation: factory.generation,
        };
        // This private opt-in keeps one actual carrier by default. Public
        // production registration and configurable limits remain separately gated.
        let prepared = ::trojan::component::mux::PreparedMux::from_options_refs(
            input_refs,
            &self.mux_pool,
            resources
                .candidate()
                .and_then(|candidate| candidate.source_dir()),
            binding,
            NonZeroUsize::new(1).unwrap(),
            Duration::from_secs(5),
        )
        .map_err(|source| {
            zero_config::ConfigError::InvalidOutbound(format!("outbound `{}`: {source}", input.tag))
        })?;
        let owner = resources.share(
            "trojan.mux.actual_pool",
            factory,
            Arc::new(pool::Options {
                pool: prepared.pool_options().clone(),
                tag: input.tag.clone(),
            }),
        )?;
        Ok(Options::Mux(Arc::new(prepared.bind(owner))))
    }
}
