//! Pool lookup without retaining the registry from a prepared relay prefix.
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use zero_transport::split_http::XhttpClientPool;
#[derive(Debug, Default)]
pub(super) struct Pools(Mutex<State>);
#[derive(Debug, Default)]
struct State {
    generation: Arc<()>,
    pools: HashMap<[u8; 32], XhttpClientPool>,
    grpc: HashMap<[u8; 32], zero_transport::grpc::GrpcPool>,
    hysteria: HashMap<[u8; 32], zero_transport::hysteria::Pool>,
}
#[derive(Clone, Debug, Default)]
pub(in crate::transport) struct PoolAccess {
    owner: Weak<Pools>,
    generation: Weak<()>,
}
impl Pools {
    pub(super) fn access(self: &Arc<Self>) -> PoolAccess {
        PoolAccess {
            owner: Arc::downgrade(self),
            generation: Arc::downgrade(&self.0.lock().unwrap().generation),
        }
    }
    pub(super) fn retire(&self) {
        let mut state = self.0.lock().unwrap();
        // A real retirement gets a distinct owner epoch, including A -> B -> A.
        // Old accesses retain only Weak identity, never an epoch history.
        state.generation = Arc::new(());
        for (_, pool) in state.grpc.drain() {
            pool.retire();
        }
        for (_, pool) in state.hysteria.drain() {
            pool.retire();
        }
        for (_, pool) in state.pools.drain() {
            pool.retire();
        }
    }
}
impl PoolAccess {
    /// Admission for a retained opener, not a resource-close receipt. Old
    /// one-shot finishing flows retain their existing private-pool behavior.
    pub(in crate::transport) fn check_current(&self) -> Result<(), zero_transport::RuntimeError> {
        let owner = self.owner.upgrade().ok_or_else(|| {
            zero_transport::RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "VLESS relay pool owner is gone",
            ))
        })?;
        let state = owner.0.lock().unwrap();
        if !Weak::ptr_eq(&Arc::downgrade(&state.generation), &self.generation) {
            return Err(zero_transport::RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "VLESS relay pool owner was retired",
            )));
        }
        Ok(())
    }

    pub(in crate::transport) fn pool(
        &self,
        tag: &str,
        identity: &str,
        config: zero_traits::SplitHttpXmux,
    ) -> XhttpClientPool {
        let mut hash = blake3::Hasher::new();
        hash.update(&(tag.len() as u64).to_be_bytes());
        hash.update(tag.as_bytes());
        hash.update(identity.as_bytes());
        if let Some(owner) = self.owner.upgrade() {
            let mut state = owner.0.lock().unwrap();
            // An old prepared flow may finish after reload, but must not insert
            // its configuration back into the new generation's cache.
            if Weak::ptr_eq(&Arc::downgrade(&state.generation), &self.generation) {
                let key = *hash.finalize().as_bytes();
                if let Some(pool) = state.pools.get(&key) {
                    return pool.clone();
                }
                if state.pools.len() < 4096 {
                    let pool = XhttpClientPool::new(config);
                    state.pools.insert(key, pool.clone());
                    return pool;
                }
            }
        }
        XhttpClientPool::new(config)
    }
}

#[cfg(test)]
#[path = "../../../tests/transport/xhttp_pools.rs"]
mod tests;

impl PoolAccess {
    pub(in crate::transport) fn hysteria_pool(
        &self,
        tag: &str,
        identity: &str,
    ) -> zero_transport::hysteria::Pool {
        let mut hash = blake3::Hasher::new();
        hash.update(&(tag.len() as u64).to_be_bytes());
        hash.update(tag.as_bytes());
        hash.update(identity.as_bytes());
        if let Some(owner) = self.owner.upgrade() {
            let mut state = owner.0.lock().unwrap();
            if Weak::ptr_eq(&Arc::downgrade(&state.generation), &self.generation)
                && state.hysteria.len() < 4096
            {
                return state
                    .hysteria
                    .entry(*hash.finalize().as_bytes())
                    .or_default()
                    .clone();
            }
        }
        Default::default()
    }
    pub(in crate::transport) fn grpc_pool(
        &self,
        tag: &str,
        identity: &str,
    ) -> zero_transport::grpc::GrpcPool {
        let mut hash = blake3::Hasher::new();
        hash.update(&(tag.len() as u64).to_be_bytes());
        hash.update(tag.as_bytes());
        hash.update(identity.as_bytes());
        if let Some(owner) = self.owner.upgrade() {
            let mut state = owner.0.lock().unwrap();
            if Weak::ptr_eq(&Arc::downgrade(&state.generation), &self.generation)
                && state.grpc.len() < 4096
            {
                return state
                    .grpc
                    .entry(*hash.finalize().as_bytes())
                    .or_default()
                    .clone();
            }
        }
        Default::default()
    }
}
