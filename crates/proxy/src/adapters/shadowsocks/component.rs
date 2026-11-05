//! Cold config projection into protocol-owned options and one canonical replay owner.
use super::ShadowsocksAdapter;
use crate::component::{background::stateless, RegistrationError, ResourcePreparation};
use shadowsocks::component::{ReplayFactory, StreamOptions, TcpPreparation};
use std::sync::Arc;
use zero_config::{ConfigError, OutboundConfig, OutboundProtocolConfig};

pub(crate) fn native_input(input: &OutboundConfig) -> bool {
    matches!(
        &input.protocol,
        OutboundProtocolConfig::Shadowsocks { plugin: None, .. }
    )
}
impl ShadowsocksAdapter {
    pub(crate) fn component_options(
        &self,
        input: &OutboundConfig,
        resources: &mut ResourcePreparation<'_>,
        replay_factory: Arc<ReplayFactory>,
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
        let prepared = self
            .outbound_state
            .prepare_tcp(TcpPreparation {
                server,
                port: *port,
                protocol: super::ShadowsocksOutboundOptionsRef { cipher, password },
                replay_policy: *replay_attack,
                limits: *state_limits,
                plugin: plugin.as_ref(),
                dial: input.dial.to_policy(),
            })
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
        let options = prepared.bind(replay).map_err(options_error)?;
        Ok(stateless::Options::new(
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
            std::num::NonZeroUsize::new(4).unwrap(),
        ))
    }
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
}
