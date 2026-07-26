//! Principal-scoped cancellation registrations and dispatch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

type CancellationCallback = Box<dyn FnOnce(String) + Send + 'static>;

#[derive(Default)]
pub(crate) struct PrincipalCancellationRegistry {
    inner: Arc<PrincipalCancellationRegistryInner>,
}

#[derive(Default)]
struct PrincipalCancellationRegistryInner {
    next_id: AtomicU64,
    callbacks: Mutex<HashMap<String, HashMap<u64, CancellationCallback>>>,
}

impl std::fmt::Debug for PrincipalCancellationRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrincipalCancellationRegistry")
            .finish()
    }
}

impl PrincipalCancellationRegistry {
    pub(crate) fn register<F>(
        &self,
        principal_key: &str,
        callback: F,
    ) -> PrincipalCancellationRegistration
    where
        F: FnOnce(String) + Send + 'static,
    {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner
            .callbacks
            .lock()
            .expect("principal cancellation registry lock poisoned")
            .entry(principal_key.to_owned())
            .or_default()
            .insert(id, Box::new(callback));
        PrincipalCancellationRegistration {
            inner: Arc::downgrade(&self.inner),
            principal_key: principal_key.to_owned(),
            id,
        }
    }

    pub(crate) fn cancel(&self, principal_key: &str, reason: &str) {
        let callbacks = self
            .inner
            .callbacks
            .lock()
            .expect("principal cancellation registry lock poisoned")
            .remove(principal_key);
        if let Some(callbacks) = callbacks {
            for callback in callbacks.into_values() {
                callback(reason.to_owned());
            }
        }
    }
}

pub struct PrincipalCancellationRegistration {
    inner: Weak<PrincipalCancellationRegistryInner>,
    principal_key: String,
    id: u64,
}

impl std::fmt::Debug for PrincipalCancellationRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PrincipalCancellationRegistration")
            .field("principal_key", &self.principal_key)
            .field("id", &self.id)
            .finish()
    }
}

impl Drop for PrincipalCancellationRegistration {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut callbacks = inner
            .callbacks
            .lock()
            .expect("principal cancellation registry lock poisoned");
        let Some(principal_callbacks) = callbacks.get_mut(&self.principal_key) else {
            return;
        };
        principal_callbacks.remove(&self.id);
        if principal_callbacks.is_empty() {
            callbacks.remove(&self.principal_key);
        }
    }
}
