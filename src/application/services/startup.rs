//! One concrete application startup slot, outside the cancellable initializer.
use super::*;

pub(super) struct ApplicationStartupSlot {
    owner: std::sync::Mutex<Option<Arc<ApplicationServices>>>,
    #[cfg(test)]
    pub(super) pause: std::sync::Mutex<Option<Arc<StartupPause>>>,
}
impl ApplicationStartupSlot {
    pub(super) const fn new() -> Self {
        Self {
            owner: std::sync::Mutex::new(None),
            #[cfg(test)]
            pause: std::sync::Mutex::new(None),
        }
    }
    pub(super) fn install(&self, owner: Arc<ApplicationServices>) -> Result<(), std::io::Error> {
        let mut slot = self.owner.lock().expect("application startup slot");
        if slot.is_some() {
            return Err(std::io::Error::other(
                "original application startup still owns unresolved preparation/cleanup",
            ));
        }
        *slot = Some(owner);
        Ok(())
    }
    pub(super) fn retain(&self, original: Arc<ApplicationServices>) -> Result<(), std::io::Error> {
        let mut slot = self.owner.lock().expect("application startup slot");
        if slot
            .as_ref()
            .is_some_and(|owner| !Arc::ptr_eq(owner, &original))
        {
            return Err(std::io::Error::other(
                "foreign original application still owns startup",
            ));
        }
        *slot = Some(original);
        Ok(())
    }
    pub(super) fn clear(&self, original: &Arc<ApplicationServices>) {
        let mut slot = self.owner.lock().expect("application startup slot");
        if slot
            .as_ref()
            .is_some_and(|owner| Arc::ptr_eq(owner, original))
        {
            *slot = None;
        }
    }
    #[cfg(test)]
    pub(super) fn original(&self) -> Option<Arc<ApplicationServices>> {
        self.owner.lock().unwrap().clone()
    }
}

pub(super) static APPLICATION_STARTUP: ApplicationStartupSlot = ApplicationStartupSlot::new();

#[cfg(test)]
pub(super) struct StartupPause {
    pub(super) prepared: tokio::sync::Notify,
    pub(super) resume: tokio::sync::Semaphore,
}
