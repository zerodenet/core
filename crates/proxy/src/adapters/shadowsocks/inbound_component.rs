//! Native source selection and cold protocol-owned profile preparation.
use super::*;
impl ShadowsocksAdapter {
    pub(crate) fn inbound_options(
        &self,
        inbound: &InboundConfig,
        replay_max_salts: usize,
    ) -> Result<
        ::shadowsocks::component::inbound::listener::Options,
        crate::component::RegistrationError,
    > {
        let InboundProtocolConfig::Shadowsocks {
            password,
            identity_password,
            users,
            cipher,
            replay_attack,
            state_limits,
            plugin,
            ..
        } = &inbound.protocol
        else {
            return Err(zero_config::ConfigError::InvalidInbound(
                "Shadowsocks source requires its own inbound config".into(),
            )
            .into());
        };
        let users = inbound_user_refs(cipher, password, users);
        let prepared = self
            .inbound_profiles
            .prepare_with_identity(
                &inbound.tag,
                cipher,
                identity_password.as_deref(),
                &users,
                *replay_attack,
                *state_limits,
            )
            .and_then(|prepared| prepared.with_replay_budget_limit(replay_max_salts))
            .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))?;
        let endpoint =
            zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port);
        let listen = zero_core::request::Destination::new(
            match endpoint.map(|endpoint| endpoint.ip()) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(inbound.listen.address.clone()),
            },
            inbound.listen.port,
        );
        let identity = serde_json::to_vec(inbound)
            .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))?;
        Ok(
            ::shadowsocks::component::inbound::listener::Options::new(listen, prepared, &identity)
                .with_plugin(plugin.clone()),
        )
    }
}
