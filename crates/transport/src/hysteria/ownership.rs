//! One owning tag's actual pool census survives profile changes and reload.
//! Legacy exposure has no original-close proof; a new cache key cannot erase it.
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
#[derive(Default)]
pub struct OwnershipFamily {
    legacy: AtomicBool,
    native: Mutex<Vec<super::managed::OwnerRecord>>,
}
impl std::fmt::Debug for OwnershipFamily {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HysteriaOwnershipFamily")
    }
}
impl OwnershipFamily {
    pub(super) fn native_eligible(&self) -> bool {
        !self.legacy.load(Ordering::Acquire)
    }
    pub(super) fn enter_legacy(&self) -> Result<(), ChannelError> {
        let native = self.native.lock().unwrap();
        if native.iter().any(|owner| !owner.is_closed()) {
            return Err(error());
        }
        self.legacy.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) fn install(
        &self,
        owner: &Arc<ManagedPool>,
    ) -> Result<Vec<super::managed::OwnerRecord>, ChannelError> {
        let mut native = self.native.lock().unwrap();
        if self.legacy.load(Ordering::Acquire)
            || native.iter().any(|previous| !previous.is_closed())
        {
            return Err(error());
        }
        let previous = std::mem::take(&mut *native);
        native.push(super::managed::OwnerRecord::new(owner));
        drop(native);
        Ok(previous)
    }
}
fn error() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
}
impl Pool {
    /// Same old library-generation behavior, with the owning exposure census
    /// preserved. Active host modes are never replaced by this reload helper.
    pub fn next_legacy_generation(&self) -> Option<Self> {
        if self.has_host_owner() {
            return None;
        }
        Some(Self::in_family(self.0.family.clone()))
    }
}
