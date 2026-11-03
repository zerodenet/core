//! Cold config projection into protocol-owned options and one canonical replay owner.
use super::ShadowsocksAdapter;
use crate::component::{RegistrationError, ResourcePreparation};
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
    ) -> Result<StreamOptions, RegistrationError> {
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
        prepared.bind(replay).map_err(options_error)
    }
}
fn options_error(error: impl std::fmt::Display) -> RegistrationError {
    RegistrationError::Options(ConfigError::InvalidOutbound(error.to_string()))
}
