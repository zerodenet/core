pub(in crate::transport) mod browser_dialer;
pub(in crate::transport) mod preconnect;
pub(in crate::transport) mod xhttp;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use zero_core::Error;

use super::VlessInboundUserRef;

#[derive(Debug, Clone, Default)]
pub struct VlessTransportRuntime {
    browser_dialers: browser_dialer::Registry,
    hysteria_pools: Arc<Mutex<HashMap<[u8; 32], zero_transport::hysteria::Pool>>>,
    xhttp_pools: Arc<xhttp::Pools>,
    grpc_pools: Arc<Mutex<HashMap<[u8; 32], zero_transport::grpc::GrpcPool>>>,
    mux_pool: crate::mux_pool::MuxConnectionPool,
    portals: crate::reverse::PortalRegistry,
    encryption_pools: Arc<Mutex<HashMap<[u8; 32], crate::mux_pool::MuxConnectionPool>>>,
    inbound_profiles: Arc<Mutex<HashMap<String, crate::inbound::VlessInboundProfile>>>,
    preconnect: preconnect::Registry,
    target_probes: crate::reality::target::ProbeRegistry,
}

impl VlessTransportRuntime {
    pub fn on_config_reloaded(&self) {
        self.browser_dialers.retire();
        self.portals.retire();
        self.mux_pool.evict_all();
        self.xhttp_pools.retire();
        self.preconnect.retire();
        self.target_probes.retire();
        for (_, pool) in self.hysteria_pools.lock().unwrap().drain() {
            pool.retire();
        }
        for (_, pool) in self.grpc_pools.lock().unwrap().drain() {
            pool.retire();
        }
        for (_, pool) in self
            .encryption_pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
        {
            pool.evict_all();
        }
    }

    pub(super) fn browser_dialer(
        &self,
        settings: zero_traits::BrowserDialerSettings,
    ) -> browser_dialer::Access {
        self.browser_dialers.access(settings)
    }

    pub fn reverse_portal(&self, tag: &str) -> crate::reverse::Portal {
        self.portals.portal(tag)
    }

    pub(super) fn hysteria_pool(
        &self,
        tag: &str,
        identity: &str,
        profile: &zero_transport::hysteria::Profile,
    ) -> zero_transport::hysteria::Pool {
        let mut hash = blake3::Hasher::new();
        hash.update(&(tag.len() as u64).to_be_bytes());
        hash.update(tag.as_bytes());
        hash.update(&profile.cache_identity());
        hash.update(identity.as_bytes());
        let mut pools = self.hysteria_pools.lock().unwrap();
        if pools.len() >= 4096 {
            return Default::default();
        }
        pools
            .entry(*hash.finalize().as_bytes())
            .or_default()
            .clone()
    }
    pub(super) fn grpc_pool(&self, tag: &str, identity: &str) -> zero_transport::grpc::GrpcPool {
        let mut hash = blake3::Hasher::new();
        hash.update(&(tag.len() as u64).to_be_bytes());
        hash.update(tag.as_bytes());
        hash.update(identity.as_bytes());
        let mut pools = self.grpc_pools.lock().unwrap();
        if pools.len() >= 4096 {
            return Default::default();
        }
        pools
            .entry(*hash.finalize().as_bytes())
            .or_default()
            .clone()
    }
    pub(super) fn xhttp_pool(
        &self,
        tag: &str,
        identity: &str,
        config: zero_traits::SplitHttpXmux,
    ) -> zero_transport::split_http::XhttpClientPool {
        self.xhttp_pools.access().pool(tag, identity, config)
    }
    pub(super) fn xhttp_pool_access(&self) -> xhttp::PoolAccess {
        self.xhttp_pools.access()
    }
    pub(super) fn mux_pool(&self) -> crate::mux_pool::MuxConnectionPool {
        self.mux_pool.clone()
    }

    pub(super) fn preconnect_pool(
        &self,
        tag: &str,
        carrier_identity: [u8; 32],
        capacity: u32,
    ) -> Option<preconnect::Access> {
        self.preconnect.create(tag, carrier_identity, capacity)
    }

    pub(super) fn target_probes(&self) -> crate::reality::target::ProbeAccess {
        self.target_probes.access()
    }

    pub(super) fn encryption_mux_pool(
        &self,
        tag: &str,
        config: &str,
    ) -> crate::mux_pool::MuxConnectionPool {
        let mut hash = blake3::Hasher::new();
        hash.update(tag.as_bytes());
        hash.update(&[0]);
        hash.update(config.as_bytes());
        self.encryption_pools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(*hash.finalize().as_bytes())
            .or_default()
            .clone()
    }

    pub fn replace_inbound_profile(
        &self,
        tag: &str,
        users: &[VlessInboundUserRef<'_>],
    ) -> Result<crate::inbound::VlessInboundProfile, Error> {
        let mut profiles = self
            .inbound_profiles
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(profile) = profiles.get(tag) {
            profile.replace_config_users(users.iter().copied())?;
            return Ok(profile.clone());
        }

        let profile =
            crate::inbound::VlessInboundProfile::from_config_users(users.iter().copied())?
                .with_portals(self.portals.clone());
        profiles.insert(tag.to_owned(), profile.clone());
        Ok(profile)
    }
}
