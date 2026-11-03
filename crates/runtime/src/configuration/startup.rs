//! A concrete finite startup slot; successful independent kernels remain independent.
use super::ConfigurationSource;
use std::{
    io,
    sync::{Arc, Mutex},
};
struct ConfigurationPreparationStartupSlot {
    owner: Mutex<Option<Arc<ConfigurationSource>>>,
}
impl ConfigurationPreparationStartupSlot {
    const fn new() -> Self {
        Self {
            owner: Mutex::new(None),
        }
    }
    fn confirm_closed(&self) -> io::Result<()> {
        let slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(original) = slot.as_ref() {
            original.io.ensure_available()?;
            return Err(io::Error::other(
                "original configuration startup was interrupted before ownership transfer",
            ));
        }
        Ok(())
    }
    fn retain_unknown(&self, source: Arc<ConfigurationSource>) {
        let mut slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        // Never replace another actual debt. The same source's finite cache
        // attempt and Root bootstrap slot retain any additional original.
        if slot.is_none() {
            *slot = Some(source);
        }
    }
    fn with_cache_source<T>(
        &self,
        source: Arc<ConfigurationSource>,
        prepare: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let mut slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(original) = slot.as_ref() {
            original.io.ensure_available()?;
            return Err(io::Error::other(
                "original configuration startup was interrupted before ownership transfer",
            ));
        }
        *slot = Some(source.clone());
        let result = prepare();
        // A successful cache begin hands the actual owner to that source's
        // concrete retained attempt, not to an async borrow of this cold gate.
        if !source.io.has_retained_failure() {
            *slot = None;
        }
        result
    }
    fn release_confirmed(&self, source: &Arc<ConfigurationSource>) -> io::Result<()> {
        let mut slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(original) = slot.as_ref() else {
            return Ok(());
        };
        if !Arc::ptr_eq(original, source) {
            return Err(io::Error::other("foreign configuration startup owner"));
        }
        // Cache namespace state is already resolved by its caller. Inspect the
        // original IO slot only; never clear it from cancellation or Drop.
        source.io.ensure_available()?;
        *slot = None;
        Ok(())
    }
    fn with_source<T>(
        &self,
        source: Arc<ConfigurationSource>,
        prepare: impl FnOnce() -> T,
    ) -> io::Result<T> {
        // Healthy concurrent startup callers wait on this synchronous short gate.
        let mut slot = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(original) = slot.as_ref() {
            original.io.ensure_available()?;
            return Err(io::Error::other(
                "original configuration startup was interrupted before ownership transfer",
            ));
        }
        *slot = Some(source.clone());
        let result = prepare();
        if source.io.ensure_available().is_ok()
            || (!source.io.has_retained_failure() && source.has_retained_cache())
        {
            *slot = None;
        }
        Ok(result)
    }
}
static STARTUP: ConfigurationPreparationStartupSlot = ConfigurationPreparationStartupSlot::new();
pub(super) fn with_source<T>(
    source: Arc<ConfigurationSource>,
    prepare: impl FnOnce() -> T,
) -> io::Result<T> {
    STARTUP.with_source(source, prepare)
}

pub(super) fn release_confirmed(source: &Arc<ConfigurationSource>) -> io::Result<()> {
    STARTUP.release_confirmed(source)
}

pub(super) fn confirm_closed() -> io::Result<()> {
    STARTUP.confirm_closed()
}
pub(super) fn retain_unknown(source: Arc<ConfigurationSource>) {
    STARTUP.retain_unknown(source);
}
pub(super) fn with_cache_source<T>(
    source: Arc<ConfigurationSource>,
    prepare: impl FnOnce() -> io::Result<T>,
) -> io::Result<T> {
    STARTUP.with_cache_source(source, prepare)
}

#[cfg(test)]
#[path = "../tests/configuration_startup.rs"]
mod tests;
