use crate::client::ClientPool;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Weak},
};
use zero_core::{request::Destination, Error};
use zero_platform_tokio::contract::network::UpstreamBinding;

use crate::client::pool::profile::CarrierPlan;
pub use crate::client::pool::profile::PoolProfile;

/// The same actual pool, a whole immutable profile set, and one total budget.
#[derive(Clone)]
pub struct PoolOptions {
    pub(in crate::component) pool: Weak<ClientPool>,
    pub(in crate::component) limit: NonZeroUsize,
    pub(in crate::component) identity: Arc<[u8]>,
    pub(in crate::component) controlled: bool,
    pub(crate) carriers: Arc<[CarrierPlan]>,
    pub(crate) profiles: Option<Arc<[PoolProfile]>>,
    pub(crate) dynamic: bool,
}
impl std::fmt::Debug for PoolOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MieruPoolOptions")
            .field("carrier_limit", &self.limit)
            .field("carriers", &self.carriers.len())
            .field(
                "profiles",
                &self.profiles.as_ref().map(|profiles| profiles.len()),
            )
            .finish_non_exhaustive()
    }
}
impl ClientPool {
    pub fn component_options(self: &Arc<Self>, carrier_limit: NonZeroUsize) -> PoolOptions {
        self.pool_options(carrier_limit, true)
    }
    #[cfg(test)]
    pub(crate) fn uncontrolled_component_options(
        self: &Arc<Self>,
        carrier_limit: NonZeroUsize,
    ) -> PoolOptions {
        self.pool_options(carrier_limit, false)
    }
    fn pool_options(
        self: &Arc<Self>,
        carrier_limit: NonZeroUsize,
        controlled: bool,
    ) -> PoolOptions {
        let mut identity = (Arc::as_ptr(self) as usize).to_le_bytes().to_vec();
        identity.extend_from_slice(&carrier_limit.get().to_le_bytes());
        identity.push(u8::from(controlled));
        PoolOptions {
            pool: Arc::downgrade(self),
            limit: carrier_limit,
            identity: identity.into(),
            controlled,
            carriers: Arc::from([]),
            profiles: None,
            dynamic: false,
        }
    }
}
pub struct PoolCarrierSpec<'a> {
    pub endpoint: &'a Destination,
    pub binding: &'a UpstreamBinding,
    pub tag: Option<&'a str>,
    pub wire_endpoint: &'a Destination,
    pub datagram: bool,
}
impl PoolOptions {
    pub fn carrier_specs(&self) -> impl Iterator<Item = PoolCarrierSpec<'_>> {
        self.carriers.iter().map(|carrier| PoolCarrierSpec {
            endpoint: &carrier.endpoint,
            binding: &carrier.binding,
            tag: carrier.tag.as_deref(),
            wire_endpoint: &carrier.wire_endpoint,
            datagram: carrier.udp,
        })
    }
    /// Dynamic providers run initialization and the original driver in this
    /// same Scope, sequentially under the same task limit. A library owner
    /// must drive its actual TaskScope concurrently with role operations; body
    /// completion alone cannot substitute for the Scope's consumed join.
    pub fn with_dynamic_profiles(
        self,
        profiles: impl IntoIterator<Item = PoolProfile>,
    ) -> Result<Self, Error> {
        let mut prepared = self.with_profiles(profiles)?;
        prepared.dynamic = true;
        prepared.identity = Arc::from(crate::client::pool::profile::opaque_identity((
            "dynamic-original-carriers-v1",
            prepared.identity.as_ref(),
        )));
        Ok(prepared)
    }

    /// Freeze one TCP root recipe; assembly supplies its actual endpoint provider.
    pub fn with_background_carrier(
        mut self,
        endpoint: Destination,
        binding: UpstreamBinding,
    ) -> Result<Self, Error> {
        if !self.controlled || !self.carriers.is_empty() || self.profiles.is_some() {
            return Err(Error::Config("invalid Mieru background carrier"));
        }
        let plan = CarrierPlan::new(endpoint, binding)?;
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(format!("{:?}", plan).as_bytes());
        self.identity = identity.into();
        self.carriers = Arc::from([plan]);
        Ok(self)
    }
    /// Assembly supplies every profile, including unused credentials. Any change
    /// retires the entire actual pool, not only the role first used after reload.
    pub fn with_profiles(
        mut self,
        profiles: impl IntoIterator<Item = PoolProfile>,
    ) -> Result<Self, Error> {
        if !self.controlled || !self.carriers.is_empty() || self.profiles.is_some() {
            return Err(Error::Config("Mieru pool profiles already frozen"));
        }
        let mut profiles: Vec<_> = profiles.into_iter().collect();
        profiles.sort_by(|a, b| a.key.tag.cmp(&b.key.tag));
        if profiles.is_empty()
            || profiles
                .windows(2)
                .any(|pair| pair[0].key.tag == pair[1].key.tag)
        {
            return Err(Error::Config(
                "Mieru pool needs unique nonempty profile tags",
            ));
        }
        let mut carriers = Vec::new();
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"complete-profiles-v1");
        for profile in &profiles {
            if !carriers.contains(&profile.carrier) {
                carriers.push(profile.carrier.clone());
            }
            identity.extend_from_slice(&profile.identity());
        }
        self.identity = Arc::from(crate::client::pool::profile::opaque_identity((
            "mieru.pool",
            identity.as_slice(),
        )));
        self.carriers = carriers.into();
        self.profiles = Some(profiles.into());
        Ok(self)
    }
}
