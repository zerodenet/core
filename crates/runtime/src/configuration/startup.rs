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
        if source.io.ensure_available().is_ok() {
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

#[cfg(test)]
#[path = "../tests/configuration_startup.rs"]
mod tests;
