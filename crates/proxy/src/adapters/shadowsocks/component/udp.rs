//! The existing SS cache receives its actual Host scope and background provider.
use super::*;
use crate::component::background::{BackgroundCarrier, BackgroundNetwork};
use shadowsocks::component::udp::{UdpPoolFactory, UdpPoolOptions};
use zero_core::component::{ComponentId, NetworkGeneration};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
    network::UpstreamBinding,
};
struct Options {
    pool: Arc<UdpPoolOptions>,
    dns: Option<ComponentId>,
}
struct Factory {
    network: Arc<BackgroundNetwork>,
    tag: Arc<str>,
    wire_endpoint: zero_core::request::Destination,
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = UdpPoolFactory.describe(&options.pool);
        spec.dependencies.extend(options.dns);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if options
            .dns
            .is_some_and(|dns| context.dependency(dns).is_none())
        {
            return Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "actual SS UDP Node owner is absent",
            ));
        }
        if let Some(dns) = options.dns {
            context.dependencies = context
                .dependencies
                .iter()
                .copied()
                .filter(|instance| instance.component != dns)
                .collect::<Vec<_>>()
                .into();
        }
        context.background_carrier = Some(
            BackgroundCarrier {
                network: self.network.clone(),
                instance: context.instance(),
                initial: options.pool.binding().clone(),
                endpoint: options.pool.endpoint().clone(),
                wire_endpoint: self.wire_endpoint.clone(),
                tag: self.tag.to_string(),
            }
            .transport(),
        );
        UdpPoolFactory.build(options.pool.clone(), context)
    }
}
impl ShadowsocksAdapter {
    pub(super) fn prepare_udp(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        network: Arc<BackgroundNetwork>,
        process: Option<ComponentId>,
    ) -> Result<(ComponentId, Arc<UdpPoolOptions>), RegistrationError> {
        let leaf = super::super::transport_leaf(&self.outbound_state, input.tag(), &input.protocol)
            .ok_or_else(|| options_error("expected SS outbound UDP configuration"))?;
        let mut pool = self
            .outbound_state
            .udp_component_options(
                &leaf,
                UpstreamBinding {
                    prefix: vec![],
                    dial: input.dial.to_policy(),
                    network_generation: NetworkGeneration(network.egress.generation()),
                },
            )
            .map_err(options_error)?;
        let udp_plugin = matches!(&input.protocol,OutboundProtocolConfig::Shadowsocks {plugin:Some(plugin),..} if plugin.mode.udp());
        if udp_plugin {
            pool = pool.with_process_dependency(
                process
                    .ok_or_else(|| options_error("actual SIP UDP process dependency is absent"))?,
                shadowsocks::transport::plugin::STARTUP_TIMEOUT,
            );
        }
        pool = pool
            .with_pending_limits(shadowsocks::transport::ShadowsocksUdpPendingLimits {
                per_cache: resources
                    .candidate()
                    .map(|config| config.runtime.effective_ss_udp_max_pending_responses())
                    .unwrap_or(4096),
                total: resources
                    .candidate()
                    .map(|config| {
                        config
                            .runtime
                            .effective_ss_udp_total_max_pending_responses()
                    })
                    .unwrap_or(4096),
            })
            .map_err(options_error)?;
        let runtime = resources.candidate().map(|config| &config.runtime);
        pool = pool.with_security_budget_proposals(
            self.outbound_state
                .udp_budget_proposal(
                    runtime
                        .map(super::super::udp_budget_limits)
                        .unwrap_or_default(),
                )
                .map_err(options_error)?,
            self.outbound_state
                .legacy_replay_budget_proposal(
                    runtime
                        .map(|runtime| runtime.effective_ss_legacy_replay_max_bytes())
                        .unwrap_or(64 * 1024 * 1024),
                )
                .map_err(options_error)?,
        );
        let dns = (!udp_plugin && matches!(pool.endpoint().address, zero_core::Address::Domain(_)))
            .then(|| crate::component::native_dns::prepare_node(resources, network.clone()))
            .transpose()?;
        let pool = Arc::new(pool);
        let id = resources.share(
            "shadowsocks.udp.cache",
            Arc::new(Factory {
                network,
                tag: input.tag().into(),
                wire_endpoint: zero_core::request::Destination::new(
                    zero_core::Address::Domain(leaf.server().into()),
                    leaf.port(),
                ),
            }),
            Arc::new(Options {
                pool: pool.clone(),
                dns,
            }),
        )?;
        Ok((id, pool))
    }
}
