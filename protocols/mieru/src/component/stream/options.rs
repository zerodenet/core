use crate::{component::PoolOptions, transport::MieruOutboundOptionsRef};
use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, OnceLock},
};
use zero_core::{component::ComponentId, Error};
use zero_traits::DialPolicy;

pub struct TcpPreparation<'a> {
    pub tag: &'a str,
    pub server: &'a str,
    pub port: u16,
    pub protocol: MieruOutboundOptionsRef<'a>,
    pub dial: DialPolicy,
}
pub struct PreparedTcp {
    pub(super) pool: PoolOptions,
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) username: String,
    pub(super) password: String,
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
        if input.protocol.udp {
            return Err(Error::Unsupported(
                "native Mieru Stream requires TCP underlay",
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
        // Process-local keyed equality includes credentials without exposing them
        // in descriptors or Debug. The same actual pool and bound are retained.
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            (self.pool.as_ptr() as usize).hash(&mut hash);
            self.limit.hash(&mut hash);
            input.tag.hash(&mut hash);
            input.server.hash(&mut hash);
            input.port.hash(&mut hash);
            input.protocol.username.hash(&mut hash);
            input.protocol.password.hash(&mut hash);
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
            options: input.protocol.options.clone(),
            dial: input.dial,
            identity: identity.into(),
        })
    }
}
impl PreparedTcp {
    pub fn pool_options(&self) -> &PoolOptions {
        &self.pool
    }
    pub fn bind(self, pool_owner: ComponentId) -> StreamOptions {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(&pool_owner.0.to_le_bytes());
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
