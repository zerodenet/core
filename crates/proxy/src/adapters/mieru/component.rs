//! Project the complete candidate once into the adapter's actual shared pool.
use super::MieruAdapter;
use crate::component::{
    background::{BackgroundCarrier, BackgroundNetwork},
    RegistrationError, ResourcePreparation,
};
use ::mieru::{
    component::{PoolFactory, PoolOptions, PoolProfile, StreamOptions, TcpPreparation},
    transport::MieruOutboundOptionsRef,
};
use std::{num::NonZeroUsize, sync::Arc};
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};
use zero_core::component::NetworkGeneration;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};

pub(crate) struct NativePoolOptions {
    pool: Arc<PoolOptions>,
    dns: zero_core::component::ComponentId,
}
pub(crate) struct Factory {
    pub(crate) network: Arc<BackgroundNetwork>,
    // One cold assembly stamp, never per-decode moving network state. Dynamic
    // acquisition must still reselect the actual live provider for every open.
    generation: NetworkGeneration,
}
impl Factory {
    pub(crate) fn new(network: Arc<BackgroundNetwork>) -> Self {
        let generation = NetworkGeneration(network.egress.generation());
        Self {
            network,
            generation,
        }
    }
}
impl ComponentFactory for Factory {
    type Options = NativePoolOptions;
    fn describe(&self, options: &NativePoolOptions) -> ComponentSpec {
        let mut spec = PoolFactory.describe(&options.pool);
        spec.dependencies.push(options.dns);
        spec
    }
    fn build(
        &self,
        options: Arc<NativePoolOptions>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if context.dependencies.len() != 1 || context.dependencies[0].component != options.dns {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        // The proxy consumes its actual DNS assembly dependency. The protocol's
        // own roleless pool contract has no protocol-local dependencies.
        context.dependencies = Arc::from([]);
        let upstreams = options
            .pool
            .carrier_specs()
            .map(|spec| {
                BackgroundCarrier {
                    network: self.network.clone(),
                    instance: context.instance(),
                    initial: spec.binding.clone(),
                    endpoint: spec.endpoint.clone(),
                    wire_endpoint: spec.wire_endpoint.clone(),
                    tag: spec.tag.unwrap_or("").into(),
                }
                .transport()
            })
            .collect();
        PoolFactory.build_with_carriers(options.pool.clone(), context, upstreams)
    }
}
impl MieruAdapter {
    pub(crate) fn component_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        factory: Arc<Factory>,
    ) -> Result<StreamOptions, RegistrationError> {
        let candidate = resources
            .candidate()
            .ok_or_else(|| options_error("complete Mieru candidate is required"))?;
        let generation = factory.generation;
        let profiles = candidate
            .outbounds
            .iter()
            .filter_map(preparation)
            .map(|input| {
                let binding = UpstreamBinding {
                    prefix: vec![],
                    dial: input.dial.clone(),
                    network_generation: generation,
                };
                PoolProfile::new(input, binding).map_err(options_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        // One existing conservative policy bound for all identities and both
        // underlays together. A profile never creates a separate native pool.
        let limit = NonZeroUsize::new(
            ::mieru::client::ClientPoolPolicy::default().max_connections_per_identity,
        )
        .unwrap();
        let options = self
            .pool
            .component_options(limit)
            .with_dynamic_profiles(profiles)
            .map_err(options_error)?;
        let prepared = options
            .prepare_tcp(
                preparation(input).ok_or_else(|| options_error("expected Mieru outbound"))?,
            )
            .map_err(options_error)?;
        let dns = crate::component::native_dns::prepare(resources, factory.network.clone())?;
        let shared = resources
            .share(
                "mieru.client_pool",
                factory,
                Arc::new(NativePoolOptions {
                    pool: Arc::new(options),
                    dns,
                }),
            )
            .map_err(|error| {
                RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
            })?;
        Ok(prepared.bind(shared))
    }
}
fn preparation(input: &OutboundConfig) -> Option<TcpPreparation<'_>> {
    let OutboundProtocolConfig::Mieru {
        server,
        port,
        username,
        password,
        transport,
        options,
    } = &input.protocol
    else {
        return None;
    };
    Some(TcpPreparation {
        tag: input.tag(),
        server,
        port: *port,
        dial: input.dial.to_policy(),
        protocol: MieruOutboundOptionsRef {
            username: username.as_deref().unwrap_or(password),
            password,
            options,
            udp: matches!(transport, zero_config::MieruTransport::Udp),
        },
    })
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
}
