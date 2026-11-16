//! Cold assembly of the existing VLESS preconnection entry into this one Host.
use super::*;
use crate::component::{
    background::{self, pool, BackgroundCarrier},
    RegistrationError, ResourcePreparation,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
pub(crate) fn eligible(input: &OutboundConfig) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Vless { flow, .. } if flow.as_deref().is_none_or(|value|value=="zero-aead-v1")
    ) && carrier_eligible(input)
}
pub(super) fn carrier_eligible(input: &OutboundConfig) -> bool {
    let mut plain = input.clone();
    let OutboundProtocolConfig::Vless {
        testpre,
        mux_concurrency,
        xudp_concurrency,
        ..
    } = &mut plain.protocol
    else {
        return false;
    };
    if *testpre == 0 || mux_concurrency.is_some() || xudp_concurrency.is_some() {
        return false;
    }
    *testpre = 0;
    super::owned_input(&plain)
        || super::grpc::eligible(&plain)
        || super::mux::lower::eligible(&plain)
}
struct Options {
    prepared: Arc<::vless::component::preconnect::PreparedPreconnect>,
    tag: String,
    wire: zero_core::request::Destination,
    lower: Option<(
        zero_core::component::ComponentId,
        super::mux::lender::Source,
    )>,
}
struct Factory {
    background: Arc<pool::Factory>,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ::vless::component::preconnect::PoolFactory.describe(&options.prepared);
        let mut identity = spec.static_identity.to_vec();
        let wire = format!("{:?}", options.wire);
        identity.extend_from_slice(&(wire.len() as u64).to_le_bytes());
        identity.extend_from_slice(wire.as_bytes());
        spec.static_identity = identity.into();
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let instance = context.instance();
        let provider = Arc::new(BackgroundCarrier {
            network: self.background.network.clone(),
            instance,
            initial: options.prepared.pool_options().binding().clone(),
            endpoint: options.prepared.pool_options().endpoint().clone(),
            wire_endpoint: options.wire.clone(),
            tag: options.tag.clone(),
        });
        context.background_carrier = Some(match &options.lower {
            Some((dependency, source)) => {
                let expected = context
                    .dependency(*dependency)
                    .filter(|actual| *actual != instance)
                    .ok_or_else(|| {
                        zero_platform_tokio::contract::ChannelError::new(
                            zero_platform_tokio::contract::Stage::Prepare,
                            zero_platform_tokio::contract::Reason::StalePlan,
                            zero_platform_tokio::contract::Impact::Component,
                        )
                    })?;
                super::mux::lender::transport(
                    provider,
                    options.prepared.pool_options().binding().clone(),
                    expected,
                    source.clone(),
                )
            }
            None => zero_platform_tokio::contract::network::UpstreamTransport::new(
                options.prepared.pool_options().binding().clone(),
                provider,
            ),
        });
        ::vless::component::preconnect::PoolFactory.build_with_ech_resolver(
            options.prepared.clone(),
            context,
            background::grpc::tls_resolver(self.background.network.clone(), instance),
        )
    }
}
impl super::super::VlessAdapter {
    pub(super) fn preconnect_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        background: Arc<pool::Factory>,
    ) -> Result<super::mux::Options, RegistrationError> {
        let (prepared, owner) = self.preconnect_pool_options(input, resources, background)?;
        Ok(super::mux::Options::Pooled(Arc::new(prepared.bind(owner))))
    }
    pub(super) fn preconnect_pool_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        background: Arc<pool::Factory>,
    ) -> Result<
        (
            Arc<::vless::component::preconnect::PreparedPreconnect>,
            zero_core::component::ComponentId,
        ),
        RegistrationError,
    > {
        let OutboundProtocolConfig::Vless {
            server, port, tls, ..
        } = &input.protocol
        else {
            unreachable!("owning VLESS projection");
        };
        let runtime = resources
            .candidate()
            .map(|config| config.runtime.clone())
            .unwrap_or_default();
        let bound = |n| {
            NonZeroUsize::new(n).ok_or_else(|| {
                ConfigError::InvalidRuntime("zero preconnect resource budget".into())
            })
        };
        let refs = super::super::outbound_options(input.tag(), (server, *port), &input.protocol)
            .expect("owning VLESS projection");
        let mut prepared =
            ::vless::component::preconnect::PreparedPreconnect::from_owned_options_refs(
                refs,
                &self.runtime,
                resources.source_dir(),
                UpstreamBinding {
                    prefix: vec![],
                    dial: input.dial.to_policy(),
                    network_generation: background.generation,
                },
                bound(runtime.effective_native_stream_max_originals())?,
                Duration::from_secs(30),
                bound(runtime.effective_file_read_max_bytes())?,
            )
            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
        if super::mux::needs_ip_backend(input) {
            let node =
                crate::component::native_dns::prepare_node(resources, background.network.clone())?;
            prepared = prepared.with_node_dns(node);
        }
        let lower = if matches!(
            &input.protocol,
            OutboundProtocolConfig::Vless { grpc: Some(_), .. }
        ) {
            let mut carrier = input.clone();
            let OutboundProtocolConfig::Vless { testpre, .. } = &mut carrier.protocol else {
                unreachable!("owning VLESS projection");
            };
            *testpre = 0;
            let (grpc, dependency) = self.grpc_pool_options(&carrier, resources, &background)?;
            let source = super::mux::lender::Source::Grpc(Arc::new(grpc.pool_options().clone()));
            prepared = prepared.with_carrier_lender(dependency);
            Some((dependency, source))
        } else if super::mux::lower::eligible(input) {
            let (carrier, _, dependency) = self.carrier_pool_options(
                input,
                resources,
                background.network.clone(),
                prepared.original_limit(),
            )?;
            prepared = prepared.with_carrier_lender(dependency);
            Some((dependency, super::mux::lender::Source::Carrier(carrier)))
        } else {
            None
        };
        let file = lower.is_none() && tls.as_ref().is_some_and(|tls| tls.ca_cert_path.is_some());
        let mut inherited = false;
        if file && resources.same_source_as_previous() {
            if let Some(previous) = resources.previous_named_dependencies(input.tag()) {
                for previous in previous {
                    inherited = prepared
                        .reuse_file_material_from(*previous)
                        .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
                    if !inherited {
                        inherited = resources
                            .previous_resource_identity(previous.component)
                            .map(|identity| prepared.reuse_cold_file_material_from(identity))
                            .transpose()
                            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?
                            .unwrap_or(false);
                    }
                    if inherited {
                        break;
                    }
                }
            }
        }
        let prepared = Arc::new(prepared);
        let options = Arc::new(Options {
            lower,
            prepared: prepared.clone(),
            tag: input.tag().into(),
            wire: zero_core::request::Destination::new(
                zero_core::Address::Domain(server.clone()),
                *port,
            ),
        });
        let factory = Arc::new(Factory { background });
        prepared
            .remember_cold_file_material(&factory.describe(&options).static_identity)
            .map_err(|source| ConfigError::InvalidOutbound(source.to_string()))?;
        let owner = resources.share("vless.preconnect.actual_pool", factory, options)?;
        if file && !inherited {
            resources.require_fresh_resource(owner)?;
        }
        // Both declarations retain only cold protocol data and weak installed
        // owner identities. There is no second warming queue or compatibility pool.
        Ok((prepared, owner))
    }
}
