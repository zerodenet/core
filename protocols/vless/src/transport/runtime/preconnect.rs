use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use zero_transport::RuntimeError;

mod library;
use library::Library;

struct Pool {
    control: zero_transport::pooled_driver::PoolControl,
    #[cfg(feature = "reality")]
    native: Mutex<std::sync::Weak<crate::component::preconnect::Native>>,
    capacity: usize,
    retired: AtomicBool,
    library: Mutex<Option<Arc<Library>>>,
}

#[derive(Clone)]
pub struct Access(Arc<Pool>);

impl std::fmt::Debug for Access {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VlessPreconnectPool")
            .field("capacity", &self.0.capacity)
            .finish_non_exhaustive()
    }
}

type PoolKey = (String, [u8; 32], u32);

#[derive(Default)]
struct State {
    pools: Mutex<HashMap<PoolKey, Arc<Pool>>>,
    control: zero_transport::pooled_driver::PoolControl,
}

#[derive(Clone, Default)]
pub(super) struct Registry(Arc<State>);

impl std::fmt::Debug for Registry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VlessPreconnectRegistry")
            .finish_non_exhaustive()
    }
}

impl Registry {
    pub(super) fn create(
        &self,
        tag: &str,
        carrier_identity: [u8; 32],
        capacity: u32,
    ) -> Option<Access> {
        let configured_capacity = capacity;
        let capacity = usize::try_from(capacity).ok().filter(|value| *value > 0)?;
        let key = (tag.to_owned(), carrier_identity, configured_capacity);
        let mut pools = self
            .0
            .pools
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(pool) = pools.get(&key) {
            return Some(Access(pool.clone()));
        }
        let pool = Arc::new(Pool {
            #[cfg(feature = "reality")]
            native: Mutex::new(std::sync::Weak::new()),
            control: self.0.control.scoped(format!("{key:?}").into()),
            capacity,
            retired: false.into(),
            library: Mutex::new(None),
        });
        pools.insert(key, pool.clone());
        Some(Access(pool))
    }

    pub(super) fn retire(&self) {
        self.0.retire();
    }
}

impl State {
    fn retire(&self) {
        let mut pools = self.pools.lock().unwrap_or_else(|error| error.into_inner());
        pools.retain(|_, pool| {
            if pool
                .control
                .refresh()
                .is_ok_and(|control| control.has_host_owner())
            {
                return true;
            }
            pool.retire();
            false
        });
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let pools = self
            .pools
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        for (_, pool) in pools.drain() {
            if !pool
                .control
                .refresh()
                .is_ok_and(|control| control.has_host_owner())
            {
                pool.retire();
            }
        }
    }
}

impl Access {
    #[cfg(feature = "reality")]
    pub(crate) fn install_native(
        &self,
        native: &Arc<crate::component::preconnect::Native>,
    ) -> Result<(), RuntimeError> {
        let mut installed = self.0.native.lock().unwrap();
        if self.0.retired.load(Ordering::Acquire) {
            return Err(std::io::Error::other("preconnect entry retired before start").into());
        }
        *installed = Arc::downgrade(native);
        Ok(())
    }
    #[cfg(feature = "reality")]
    pub(crate) fn native(
        &self,
        expected: zero_core::component::ComponentInstanceId,
    ) -> Result<Arc<crate::component::preconnect::Native>, RuntimeError> {
        if self.0.retired.load(Ordering::Acquire) {
            return Err(std::io::Error::other("preconnect entry retired").into());
        }
        let owner = self.0.control.refresh()?.managed()?;
        let native = self
            .0
            .native
            .lock()
            .unwrap()
            .upgrade()
            .filter(|native| native.instance() == expected && owner.instance() == expected)
            .ok_or_else(|| std::io::Error::other("preconnect original generation unavailable"))?;
        Ok(native)
    }
    #[cfg(feature = "reality")]
    pub(crate) fn control(&self) -> &zero_transport::pooled_driver::PoolControl {
        &self.0.control
    }
    #[cfg(feature = "reality")]
    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity
    }
}

impl Pool {
    fn library(&self) -> Result<Arc<Library>, RuntimeError> {
        if self.retired.load(Ordering::Acquire) {
            return Err(std::io::Error::other("preconnect entry retired").into());
        }
        // A previous native key becomes cold only after PoolFactory consumed
        // its matching original/users/Scope receipts. A stale control cannot
        // bypass that census, and unconfirmed owners remain Owned.
        self.control.refresh()?.enter_legacy()?;
        let mut library = self.library.lock().unwrap();
        if self.retired.load(Ordering::Acquire) {
            return Err(std::io::Error::other("preconnect entry retired").into());
        }
        Ok(library
            .get_or_insert_with(|| Arc::new(Library::new(self.capacity)))
            .clone())
    }
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        #[cfg(feature = "reality")]
        if let Some(native) = self.native.lock().unwrap().upgrade() {
            native.stop();
        }
        if let Some(library) = self.library.lock().unwrap().as_ref() {
            library.retire();
        }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.retire();
    }
}

#[cfg(test)]
use library::{CONNECTION_LIFETIME, REPLENISH_DELAY};
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use zero_platform_tokio::TcpRelayStream;
#[cfg(test)]
#[path = "../../../tests/transport/preconnect.rs"]
mod tests;
