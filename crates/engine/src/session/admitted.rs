//! Sealed successful admission; weak receipts own neither Engine nor policy leases.
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
use zero_core::component::SessionId;

use crate::EngineError;

/// Only the actual successful admission operation issues this receipt.
/// Numeric identity, a minted ticket and SessionHandle are not substitutes.
#[derive(Clone, Debug)]
pub struct AdmittedSession {
    pub(super) id: SessionId,
    pub(super) issuer: Weak<AtomicU64>,
    life: Weak<AdmissionLife>,
}
struct Notification {
    registered: bool,
    callback: Option<Box<dyn FnOnce() + Send>>,
}
impl std::fmt::Debug for Notification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notification")
            .field("registered", &self.registered)
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub(crate) struct AdmissionLife {
    live: AtomicBool,
    notification: Mutex<Notification>,
}
impl AdmissionLife {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            live: AtomicBool::new(true),
            notification: Mutex::new(Notification {
                registered: false,
                callback: None,
            }),
        })
    }
    pub(crate) fn receipt(
        self: &Arc<Self>,
        id: SessionId,
        issuer: &Arc<AtomicU64>,
    ) -> AdmittedSession {
        AdmittedSession {
            id,
            issuer: Arc::downgrade(issuer),
            life: Arc::downgrade(self),
        }
    }
    /// No callback/wake/drop; safe at the registry removal linearization point.
    pub(crate) fn invalidate(&self) {
        self.live.store(false, Ordering::Release);
    }
    /// Call only after registry, policy and cancellation locks are released.
    pub(crate) fn notify(&self) {
        let callback = self.notification.lock().unwrap().callback.take();
        if let Some(callback) = callback {
            callback();
        }
    }
}
impl AdmittedSession {
    pub fn id(&self) -> SessionId {
        self.id
    }
    pub fn check_active(&self) -> Result<(), EngineError> {
        if self
            .life
            .upgrade()
            .is_some_and(|life| life.live.load(Ordering::Acquire))
        {
            Ok(())
        } else {
            Err(expired())
        }
    }
    /// One bounded host notification, separate from the original session callback.
    /// Registration is never replaceable, including after notification. If the
    /// admission already ended, invoke immediately outside all internal locks.
    pub fn on_revoked(&self, callback: impl FnOnce() + Send + 'static) -> Result<(), EngineError> {
        let Some(life) = self.life.upgrade() else {
            callback();
            return Ok(());
        };
        let callback: Box<dyn FnOnce() + Send> = Box::new(callback);
        let mut notification = life.notification.lock().unwrap();
        if notification.registered {
            drop(notification);
            return Err(EngineError::AdmissionDenied {
                reason: "admission revocation receiver already registered".into(),
            });
        }
        notification.registered = true;
        if life.live.load(Ordering::Acquire) {
            notification.callback = Some(callback);
            return Ok(());
        }
        drop(notification);
        callback();
        Ok(())
    }
    pub(crate) fn check_issuer(
        &self,
        issuer: &Arc<AtomicU64>,
        id: SessionId,
    ) -> Result<(), EngineError> {
        self.check_origin(issuer, id)?;
        self.check_active()
    }
    pub(crate) fn check_origin(
        &self,
        issuer: &Arc<AtomicU64>,
        id: SessionId,
    ) -> Result<(), EngineError> {
        if !Weak::ptr_eq(&self.issuer, &Arc::downgrade(issuer)) {
            return Err(EngineError::AdmissionDenied {
                reason: "admission belongs to a different Engine allocator".into(),
            });
        }
        if self.id != id {
            return Err(EngineError::AdmissionDenied {
                reason: "admission does not match the request id".into(),
            });
        }
        Ok(())
    }
}
fn expired() -> EngineError {
    EngineError::AdmissionDenied {
        reason: "session admission is cancelled or finalized".into(),
    }
}
