use crate::{
    component::ReplayOptions,
    shared::legacy_replay::LegacyReplay,
    transport::ShadowsocksOutboundOptionsRef,
    validation::{PluginConfig, ReplayPolicy, StateLimits},
    ShadowsocksTcpConnectConfig,
};
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use zero_core::{component::ComponentId, request::Destination, Address, Error};
use zero_traits::DialPolicy;
/// Cold protocol input; configured plugins cannot be silently discarded.
pub struct TcpPreparation<'a> {
    pub server: &'a str,
    pub port: u16,
    pub protocol: ShadowsocksOutboundOptionsRef<'a>,
    pub replay_policy: ReplayPolicy,
    pub limits: StateLimits,
    pub plugin: Option<&'a PluginConfig>,
    pub dial: DialPolicy,
}
/// Owns the already-validated framing configuration, sharing the existing state.
/// The host first canonicalizes replay_options with its actual ReplayFactory.
pub struct PreparedTcp {
    pub(super) config: ShadowsocksTcpConnectConfig,
    pub(super) endpoint: Destination,
    pub(super) dial: DialPolicy,
    pub(super) replay: Option<ReplayOptions>,
    identity: Arc<[u8]>,
}
impl fmt::Debug for PreparedTcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedShadowsocksTcp")
            .field("endpoint", &self.endpoint)
            .field("dial", &self.dial)
            .finish_non_exhaustive()
    }
}
impl PreparedTcp {
    pub(crate) fn new(
        input: TcpPreparation<'_>,
        replay: impl FnOnce(ReplayPolicy, Option<usize>) -> LegacyReplay,
    ) -> Result<Self, Error> {
        if input.plugin.is_some() {
            return Err(Error::Unsupported(
                "native Shadowsocks plugin Stream requires an actual ready-only dependency lease",
            ));
        }
        if input.server.is_empty() || input.port == 0 {
            return Err(Error::Config("Shadowsocks server and port are required"));
        }
        input
            .limits
            .validate()
            .map_err(|_| Error::Config("invalid Shadowsocks state limits"))?;
        input
            .dial
            .validate()
            .map_err(|_| Error::Config("invalid Shadowsocks dial policy"))?;
        let replay = replay(input.replay_policy, input.limits.tcp_replay_capacity);
        let config = ShadowsocksTcpConnectConfig::from_config_with_replay(
            input.protocol.cipher,
            input.protocol.password,
            replay.clone(),
        )?;
        let shared = config.is_2022().then(|| {
            ReplayOptions::new(
                replay.modern_pool(),
                input.replay_policy,
                input.limits.tcp_replay_capacity,
            )
        });
        let address = match zero_core::address::parse_ip_literal(input.server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(input.server.into()),
        };
        let endpoint = Destination::new(address, input.port);
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            endpoint.hash(&mut hash);
            input.protocol.cipher.hash(&mut hash);
            input.protocol.password.hash(&mut hash);
            format!("{:?}{:?}", input.replay_policy, input.limits).hash(&mut hash);
            input.dial.hash(&mut hash);
            // Same policy in a different adapter state must not bind to this pool.
            (Arc::as_ptr(&replay.modern_pool()) as usize).hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            config,
            endpoint,
            dial: input.dial,
            replay: shared,
            identity: identity.into(),
        })
    }
    pub fn replay_options(&self) -> Option<&ReplayOptions> {
        self.replay.as_ref()
    }
    /// Bind only the host's canonical roleless identity; no placeholder RoleId.
    pub fn bind(self, replay_owner: Option<ComponentId>) -> Result<StreamOptions, Error> {
        if self.replay.is_some() != replay_owner.is_some() {
            return Err(Error::Config(
                "Shadowsocks replay owner binding does not match its actual cipher",
            ));
        }
        let mut identity = self.identity.to_vec();
        if let Some(id) = replay_owner {
            identity.extend_from_slice(&id.0.to_le_bytes());
        }
        Ok(StreamOptions {
            prepared: self,
            replay_owner,
            identity: identity.into(),
        })
    }
}
pub struct StreamOptions {
    pub(super) prepared: PreparedTcp,
    pub(super) replay_owner: Option<ComponentId>,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for StreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShadowsocksStreamOptions")
            .field("prepared", &self.prepared)
            .field("replay_owner", &self.replay_owner)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "../../../tests/component/tcp/options.rs"]
mod tests;
