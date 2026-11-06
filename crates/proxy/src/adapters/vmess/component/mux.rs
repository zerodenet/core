mod production;
use super::super::VmessAdapter;
use crate::component::{background::pool, RegistrationError, ResourcePreparation};
pub(crate) use production::has_mux;
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
impl VmessAdapter {
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
        let mut prepared = ::vmess::component::mux::PreparedMux::from_options_refs(
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
            "vmess.mux.actual_pool",
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
