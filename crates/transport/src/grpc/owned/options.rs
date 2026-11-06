use super::*;
use crate::{
    grpc::{pool::State, GrpcPool},
    profile::{OwnedClientTlsProfile, OwnedGrpcProfile},
    RuntimeError,
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Weak},
};
use zero_core::request::Destination;
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
    pub(super) base_identity: Arc<[u8]>,
    _background_recipe: Option<Arc<Recipe>>,
    pub(super) ca: Option<Arc<CaFile>>,
    pub(super) file_stamp: u64,
    pub(super) totals: Option<RootUse>,
    pub(super) node_dns: Option<zero_core::component::ComponentId>,
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
mod file;
mod recipe;
pub(super) use file::CaFile;
impl NativePoolOptions {
    pub fn from_pool(
        pool: &GrpcPool,
        settings: NativePoolSettings,
        carrier_limit: NonZeroUsize,
        stream_limit: NonZeroUsize,
    ) -> Result<Self, RuntimeError> {
        Self::prepare_recipe(pool, settings, carrier_limit, stream_limit, false)
    }
    /// Ordinary background owner: full Node/relay selection and a strongly
    /// retained immutable cold recipe; this starts no I/O or new pool.
    pub fn for_background_pool(
        pool: &GrpcPool,
        settings: NativePoolSettings,
        carrier_limit: NonZeroUsize,
        stream_limit: NonZeroUsize,
    ) -> Result<Self, RuntimeError> {
        Self::prepare_recipe(pool, settings, carrier_limit, stream_limit, true)
            .map(Self::with_background_selection)
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
        if let Some(node_dns) = self.node_dns {
            identity.extend_from_slice(b"grpc.node-dns");
            identity.extend_from_slice(&node_dns.0.to_le_bytes());
        }
        if let Some(ca) = &self.ca {
            ca.append_identity(&mut identity);
            identity.extend_from_slice(&self.file_stamp.to_le_bytes());
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
    pub fn with_node_dns(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.node_dns = Some(dependency);
        self.refresh_identity();
        self
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
    #[cfg(feature = "tls")]
    materials: std::sync::Mutex<Vec<(ComponentInstanceId, Weak<CaFile>, u64)>>,
    #[cfg(feature = "tls")]
    cold_materials: std::sync::Mutex<std::collections::HashMap<Vec<u8>, (Weak<CaFile>, u64)>>,
}

#[derive(Clone)]
pub(super) struct RootUse {
    pub(super) dependency: zero_core::component::ComponentId,
    pub(super) proposal: super::TotalsProposal,
}
