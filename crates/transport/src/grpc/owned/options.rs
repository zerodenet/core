use super::*;
use crate::{
    grpc::{pool::State, GrpcPool},
    profile::{OwnedClientTlsProfile, OwnedGrpcProfile},
    RuntimeError,
};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
};
use zero_core::{request::Destination, Address};
use zero_traits::DialPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub(super) scoped_preparation: bool,
    pub(super) selected_background: bool,
    pub(super) settings: NativePoolSettings,
    pub(super) endpoint: Destination,
    pub(super) authority: String,
    pub(super) carrier_limit: NonZeroUsize,
    pub(super) stream_limit: NonZeroUsize,
    pub(super) identity: Arc<[u8]>,
    base_identity: Arc<[u8]>,
    pub(super) totals: Option<RootUse>,
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
        let identity = {
            let mut recipe = pool.0.recipe.lock().unwrap();
            match recipe.as_ref() {
                Some(recipe)
                    if recipe.settings == settings
                        && recipe.carriers == carrier_limit
                        && recipe.streams == stream_limit =>
                {
                    recipe.identity.clone()
                }
                Some(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "conflicting complete native gRPC recipe for the same actual pool",
                    )
                    .into());
                }
                None => {
                    static NEXT: AtomicU64 = AtomicU64::new(0);
                    let mut current = NEXT.load(Ordering::Relaxed);
                    let identity = loop {
                        let next = current.checked_add(1).ok_or_else(|| {
                            std::io::Error::other("native gRPC recipe identity exhausted")
                        })?;
                        match NEXT.compare_exchange_weak(
                            current,
                            next,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break Arc::<[u8]>::from(next.to_le_bytes()),
                            Err(observed) => current = observed,
                        }
                    };
                    *recipe = Some(Recipe {
                        settings: settings.clone(),
                        carriers: carrier_limit,
                        streams: stream_limit,
                        identity: identity.clone(),
                    });
                    identity
                }
            }
        };
        Ok(Self {
            pool: Arc::downgrade(&pool.0),
            scoped_preparation: false,
            selected_background: false,
            settings,
            endpoint,
            authority,
            carrier_limit,
            stream_limit,
            base_identity: identity.clone(),
            identity,
            totals: None,
        })
    }
    fn refresh_identity(&mut self) {
        let mut identity = self.base_identity.to_vec();
        if self.scoped_preparation {
            identity.extend_from_slice(b"grpc.scoped-preparation.v1");
        }
        if self.selected_background {
            identity.extend_from_slice(b"grpc.selected-background.v1");
        }
        if let Some(totals) = &self.totals {
            identity.extend_from_slice(&totals.dependency.0.to_le_bytes());
            identity.extend_from_slice(totals.proposal.identity());
        }
        self.identity = identity.into();
    }
    /// The same owning Scope consumes and ACKs preparation before its driver.
    pub fn with_scoped_preparation(mut self) -> Self {
        self.scoped_preparation = true;
        self.refresh_identity();
        self
    }
    /// Select this original pool's actual current provider for the full path.
    pub fn with_background_selection(mut self) -> Self {
        self.scoped_preparation = true;
        self.selected_background = true;
        self.refresh_identity();
        self
    }
    /// Bind the actual registry budget resource. The cold proposal installs no
    /// quotas; that resource must start after old consumers actually settle.
    pub fn with_root_budget(
        mut self,
        dependency: zero_core::component::ComponentId,
        proposal: super::TotalsProposal,
    ) -> Result<Self, RuntimeError> {
        if !proposal.matches_limits(self.carrier_limit, self.stream_limit) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "gRPC root and actual pool budgets differ",
            )
            .into());
        }
        self.totals = Some(RootUse {
            dependency,
            proposal,
        });
        self.refresh_identity();
        Ok(self)
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub fn dial_policy(&self) -> &DialPolicy {
        &self.settings.dial
    }
    pub fn matches(&self, instance: ComponentInstanceId) -> Result<(), ChannelError> {
        let state = self
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        let managed = state.managed(instance)?;
        if managed.options.identity != self.identity
            || managed.options.settings != self.settings
            || managed.options.carrier_limit != self.carrier_limit
            || managed.options.stream_limit != self.stream_limit
            || !managed
                .options
                .pool
                .upgrade()
                .is_some_and(|actual| Arc::ptr_eq(&actual, &state))
        {
            return Err(error(instance, Stage::Prepare, Reason::StalePlan));
        }
        Ok(())
    }
    pub async fn open(
        &self,
        instance: ComponentInstanceId,
        demand: Demand,
    ) -> Result<super::NativeGrpcStream, ChannelError> {
        self.open_observed(instance, demand, None).await
    }
    /// Observe this invocation's pending original through its actual caller.
    /// The callback is weak and is never cached with the shared H2 client.
    pub async fn open_observed(
        &self,
        instance: ComponentInstanceId,
        demand: Demand,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> Result<super::NativeGrpcStream, ChannelError> {
        self.matches(instance)?;
        let state = self
            .pool
            .upgrade()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::Revoked))?;
        state.managed(instance)?.open(demand, waiting).await
    }
}

/// One complete cold recipe per actual pool, without a history or authority to
/// start it. Its checked process-local ID cannot alias a different pool/recipe.
pub(in crate::grpc) struct Recipe {
    settings: NativePoolSettings,
    carriers: NonZeroUsize,
    streams: NonZeroUsize,
    identity: Arc<[u8]>,
}

#[derive(Clone)]
pub(super) struct RootUse {
    pub(super) dependency: zero_core::component::ComponentId,
    pub(super) proposal: super::TotalsProposal,
}
