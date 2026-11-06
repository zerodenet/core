//! Cold config projection into protocol-owned options and one canonical replay owner.
use super::ShadowsocksAdapter;
mod udp;
use crate::component::{background::stateless, RegistrationError, ResourcePreparation};
use shadowsocks::component::{ReplayFactory, StreamOptions, TcpPreparation};
use std::sync::Arc;
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Shadowsocks { plugin, .. }
            if plugin.is_none() || cfg!(any(target_os = "linux", windows))
    )
}
impl ShadowsocksAdapter {
    pub(crate) fn component_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        replay_factory: Arc<ReplayFactory>,
        node_network: Option<Arc<crate::component::background::BackgroundNetwork>>,
    ) -> Result<stateless::Options<StreamOptions>, RegistrationError> {
        let OutboundProtocolConfig::Shadowsocks {
            server,
            port,
            cipher,
            password,
            replay_attack,
            state_limits,
            plugin,
        } = &input.protocol
        else {
            return Err(options_error("expected Shadowsocks outbound"));
        };
        // The protocol plan preserves the configured plugin. Its ordinary
        // consumer uses the real Process dependency, rather than silently
        // treating the configured remote as the plugin's physical endpoint.
        let process = if let Some(plugin) = plugin {
            let network = node_network
                .as_ref()
                .ok_or_else(|| options_error("plugin requires its actual runtime network epoch"))?;
            plugin.validate().map_err(options_error)?;
            let config = resources.candidate();
            let options = self
                .outbound_state
                .plugin_component_options(server, *port, plugin.clone())
                .with_process_endpoint(zero_core::component::NetworkGeneration(
                    network.egress.generation(),
                ))
                .map_err(options_error)?
                .with_udp_budget_proposal(
                    self.outbound_state
                        .udp_budget_proposal(
                            config
                                .map(|config| super::udp_budget_limits(&config.runtime))
                                .unwrap_or_default(),
                        )
                        .map_err(options_error)?,
                )
                .with_legacy_replay_budget_proposal(
                    self.outbound_state
                        .legacy_replay_budget_proposal(
                            config
                                .map(|config| config.runtime.effective_ss_legacy_replay_max_bytes())
                                .unwrap_or(64 * 1024 * 1024),
                        )
                        .map_err(options_error)?,
                );
            Some(resources.share(
                "shadowsocks.plugin.process",
                Arc::new(shadowsocks::component::PluginFactory),
                Arc::new(options),
            )?)
        } else {
            None
        };
        let prepared = self
            .outbound_state
            .prepare_tcp(TcpPreparation {
                server,
                port: *port,
                protocol: super::ShadowsocksOutboundOptionsRef { cipher, password },
                replay_policy: *replay_attack,
                limits: *state_limits,
                // TCP-enabled SIP003 uses the actual dependency above. UDP-only
                // plugins preserve the original ordinary TCP remote path.
                plugin: None,
                dial: input.dial.to_policy(),
            })
            .map_err(options_error)?
            .with_replay_budget_limit(
                resources
                    .candidate()
                    .map(|config| config.runtime.effective_ss_tcp_replay_max_salts())
                    .unwrap_or(500_000),
            )
            .map_err(options_error)?
            .with_udp_budget_limits(
                resources
                    .candidate()
                    .map(|config| super::udp_budget_limits(&config.runtime))
                    .unwrap_or_default(),
            )
            .map_err(options_error)?;
        let prepared = prepared
            .with_legacy_replay_budget_limit(
                resources
                    .candidate()
                    .map(|config| config.runtime.effective_ss_legacy_replay_max_bytes())
                    .unwrap_or(64 * 1024 * 1024),
            )
            .map_err(options_error)?;
        let replay = prepared
            .replay_options()
            .map(|options| {
                resources.share(
                    "shadowsocks.tcp.replay",
                    replay_factory,
                    Arc::new(options.clone()),
                )
            })
            .transpose()?;
        let mut options = prepared.bind(replay).map_err(options_error)?;
        if let Some(network) = node_network.clone() {
            let (owner, pool) = self.prepare_udp(input, resources, network, process)?;
            options = options.with_udp_pool(&pool, owner).map_err(options_error)?;
        }
        let options = stateless::Options::new(
            options,
            zero_core::request::Destination::new(
                match zero_core::address::parse_ip_literal(server) {
                    Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                    Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                    None => zero_core::Address::Domain(server.clone()),
                },
                *port,
            ),
            input.dial.to_policy(),
            std::num::NonZeroUsize::new(
                resources
                    .candidate()
                    .map(|config| config.runtime.effective_native_stream_max_originals())
                    .unwrap_or(4),
            )
            .filter(|limit| limit.get() <= 4096)
            .ok_or_else(|| options_error("invalid taskless Stream original limit"))?,
        );
        let options = if plugin.as_ref().is_some_and(|plugin| plugin.mode.tcp()) {
            options.with_process(
                process.ok_or_else(|| {
                    options_error("actual configured Process dependency is missing")
                })?,
                shadowsocks::transport::plugin::STARTUP_TIMEOUT,
            )
        } else {
            options
        };
        if !plugin.as_ref().is_some_and(|plugin| plugin.mode.tcp())
            && zero_core::address::parse_ip_literal(server).is_none()
            && resources.candidate().is_some()
        {
            if let Some(network) = node_network {
                let node = crate::component::native_dns::prepare_node(resources, network)?;
                return Ok(options.with_node_dns(node));
            }
        }
        Ok(options)
    }
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
}
