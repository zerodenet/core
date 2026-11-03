pub use crate::client::pool::profile::TcpPreparation;
use crate::component::PoolOptions;
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use zero_core::{component::ComponentId, Error};
use zero_traits::DialPolicy;

pub struct PreparedTcp {
    pub(super) pool: PoolOptions,
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) username: String,
    pub(super) password: String,
    pub(super) udp: bool,
    pub(super) options: mieru_config::MieruTransportOptions,
    pub(super) dial: DialPolicy,
    identity: Arc<[u8]>,
}
impl fmt::Debug for PreparedTcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedMieruTcp")
            .field("server", &self.server)
            .field("port", &self.port)
            .field("dial", &self.dial)
            .finish_non_exhaustive()
    }
}
impl PoolOptions {
    pub fn prepare_tcp(&self, input: TcpPreparation<'_>) -> Result<PreparedTcp, Error> {
        if !self.controlled {
            return Err(Error::Unsupported(
                "native Mieru Stream requires controlled carrier ownership",
            ));
        }
        if input.server.is_empty() || input.port == 0 {
            return Err(Error::Config("Mieru server and port are required"));
        }
        input
            .protocol
            .options
            .validate()
            .map_err(|_| Error::Config("invalid Mieru transport options"))?;
        input
            .dial
            .validate()
            .map_err(|_| Error::Config("invalid Mieru dial policy"))?;
        let key = crate::client::PoolKey::new(
            input.tag,
            input.server,
            input.port,
            input.protocol.username,
            input.protocol.password,
            input.protocol.udp,
        );
        if let Some(profiles) = &self.profiles {
            if !profiles.iter().any(|profile| {
                profile.matches(&key, input.protocol.options)
                    && profile.carrier.binding.dial == input.dial
            }) {
                return Err(Error::Config(
                    "Mieru Stream is absent from the complete pool profile set",
                ));
            }
        } else if let Some(carrier) = self.carriers.first() {
            let expected = match zero_core::address::parse_ip_literal(input.server) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => {
                    return Err(Error::Unsupported(
                        "Mieru declared carrier requires its selected literal endpoint",
                    ))
                }
            };
            if carrier.endpoint != zero_core::request::Destination::new(expected, input.port)
                || carrier.binding.dial != input.dial
                || carrier.udp != input.protocol.udp
            {
                return Err(Error::Config(
                    "Mieru Stream differs from its declared background carrier",
                ));
            }
        }
        // Process-local keyed equality includes credentials without exposing them
        // in descriptors or Debug. The same actual pool and bound are retained.
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            (self.pool.as_ptr() as usize).hash(&mut hash);
            self.limit.hash(&mut hash);
            self.identity.hash(&mut hash);
            self.controlled.hash(&mut hash);
            input.tag.hash(&mut hash);
            input.server.hash(&mut hash);
            input.port.hash(&mut hash);
            input.protocol.username.hash(&mut hash);
            input.protocol.password.hash(&mut hash);
            input.protocol.udp.hash(&mut hash);
            format!("{:?}", input.protocol.options).hash(&mut hash);
            input.dial.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(PreparedTcp {
            pool: self.clone(),
            tag: input.tag.into(),
            server: input.server.into(),
            port: input.port,
            username: input.protocol.username.into(),
            password: input.protocol.password.into(),
            udp: input.protocol.udp,
            options: input.protocol.options.clone(),
            dial: input.dial,
            identity: identity.into(),
        })
    }
}
impl PreparedTcp {
    pub(crate) fn key(&self) -> crate::client::PoolKey {
        crate::client::PoolKey::new(
            &self.tag,
            &self.server,
            self.port,
            &self.username,
            &self.password,
            self.udp,
        )
    }
    pub fn pool_options(&self) -> &PoolOptions {
        &self.pool
    }
    pub fn bind(mut self, pool_owner: ComponentId) -> StreamOptions {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(&pool_owner.0.to_le_bytes());
        // The canonical pool recipe retains the complete set once. Each role
        // keeps only its own credentials and weak pool reference, not copies of
        // every other user's secrets or a historical assembly description.
        self.pool.profiles = None;
        self.pool.carriers = Arc::from([]);
        StreamOptions {
            prepared: self,
            pool_owner,
            identity: identity.into(),
        }
    }
}
pub struct StreamOptions {
    pub(super) prepared: PreparedTcp,
    pub(super) pool_owner: ComponentId,
    pub(super) identity: Arc<[u8]>,
}
impl fmt::Debug for StreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MieruStreamOptions")
            .field("prepared", &self.prepared)
            .field("pool_owner", &self.pool_owner)
            .finish_non_exhaustive()
    }
}
