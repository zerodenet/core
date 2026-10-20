use super::*;
use crate::{
    grpc::{pool::State, GrpcPool},
    profile::{OwnedClientTlsProfile, OwnedGrpcProfile},
    RuntimeError,
};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, OnceLock, Weak},
};
use zero_core::{request::Destination, Address};
use zero_traits::DialPolicy;

#[derive(Clone, Debug)]
pub struct NativePoolSettings {
    pub server: String,
    pub port: u16,
    pub grpc: OwnedGrpcProfile,
    pub tls: Option<OwnedClientTlsProfile>,
    pub dial: DialPolicy,
}
#[derive(Clone)]
pub struct NativePoolOptions {
    pub(super) pool: Weak<State>,
    pub(super) settings: NativePoolSettings,
    pub(super) endpoint: Destination,
    pub(super) authority: String,
    pub(super) carrier_limit: NonZeroUsize,
    pub(super) stream_limit: NonZeroUsize,
    pub(super) identity: Arc<[u8]>,
}
impl std::fmt::Debug for NativePoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeGrpcPoolOptions")
            .field("endpoint", &self.endpoint)
            .field("carrier_limit", &self.carrier_limit)
            .field("stream_limit", &self.stream_limit)
            .finish_non_exhaustive()
    }
}
impl NativePoolOptions {
    pub fn from_pool(
        pool: &GrpcPool,
        settings: NativePoolSettings,
        carrier_limit: NonZeroUsize,
        stream_limit: NonZeroUsize,
    ) -> Result<Self, RuntimeError> {
        let invalid = || {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid native gRPC carrier options",
            )
        };
        if settings.port == 0 || stream_limit.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(invalid().into());
        }
        settings.dial.validate().map_err(|_| invalid())?;
        let ip = zero_core::address::parse_ip_literal(&settings.server).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native gRPC currently requires a literal direct endpoint",
            )
        })?;
        let address = match ip {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let endpoint = Destination::new(address, settings.port);
        if let Some(tls) = &settings.tls {
            if tls.ca_cert_path.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "native gRPC CA paths require a File owner",
                )
                .into());
            }
            if matches!(
                tls.options.ech.source().map_err(|_| invalid())?,
                Some(zero_traits::EchConfigSource::Dns { .. })
            ) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "native gRPC ECH DNS requires its service owner",
                )
                .into());
            }
            #[cfg(feature = "tls")]
            crate::tls::PreparedClient::validate_profile(tls)?;
            #[cfg(not(feature = "tls"))]
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "native gRPC TLS is not compiled",
            )
            .into());
        }
        let authority = settings
            .tls
            .as_ref()
            .and_then(|tls| tls.server_name.as_deref())
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| std::net::SocketAddr::new(ip, settings.port).to_string());
        crate::grpc::options::request(&settings.grpc, &authority)?;
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            (Arc::as_ptr(&pool.0) as usize).hash(&mut hash);
            format!("{settings:?}").hash(&mut hash);
            carrier_limit.hash(&mut hash);
            stream_limit.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            pool: Arc::downgrade(&pool.0),
            settings,
            endpoint,
            authority,
            carrier_limit,
            stream_limit,
            identity: identity.into(),
        })
    }
    pub fn matches(&self, instance: ComponentInstanceId) -> Result<(), ChannelError> {
        let state = self
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        let managed = state.managed(instance)?;
        if managed.options.identity != self.identity {
            return Err(error(instance, Stage::Prepare, Reason::StalePlan));
        }
        Ok(())
    }
    pub async fn open(
        &self,
        instance: ComponentInstanceId,
        demand: Demand,
    ) -> Result<super::NativeGrpcStream, ChannelError> {
        self.matches(instance)?;
        let state = self
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        state.managed(instance)?.open(demand).await
    }
}
