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
mod tests {
    use super::*;
    #[test]
    fn actual_interrupted_file_operation_retains_same_startup_owner_and_blocks_next_open() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("original");
        let source = Arc::new(ConfigurationSource::new(&path));
        let slot = ConfigurationPreparationStartupSlot::new();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            slot.with_source(source.clone(), || {
                let _=source.io.with_file(||std::fs::File::create(&path), |_original|->io::Result<()> {panic!("actual synchronous preparation interrupted while holding its original file")});
            }).unwrap();
        })).is_err());
        assert!(Arc::ptr_eq(
            slot.owner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .unwrap(),
            &source
        ));
        assert!(source.ensure_available().is_err());
        let mut entered = false;
        assert!(slot
            .with_source(Arc::new(ConfigurationSource::default()), || {
                entered = true;
            })
            .is_err());
        assert!(!entered);
        let mut opened = false;
        assert!(source
            .io
            .with_file(
                || {
                    opened = true;
                    std::fs::File::open(&path)
                },
                |_| Ok(())
            )
            .is_err());
        assert!(!opened);
    }
    #[test]
    fn normal_concurrent_independent_preparations_wait_and_preserve_their_owners() {
        let slot = Arc::new(ConfigurationPreparationStartupSlot::new());
        let mut workers = Vec::new();
        for _ in 0..8 {
            let slot = slot.clone();
            workers.push(std::thread::spawn(move || {
                let source = Arc::new(ConfigurationSource::default());
                slot.with_source(source.clone(), || source.clone()).unwrap()
            }));
        }
        let sources = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        for a in 0..sources.len() {
            for b in a + 1..sources.len() {
                assert!(!Arc::ptr_eq(&sources[a], &sources[b]));
            }
        }
        assert!(slot.owner.lock().unwrap().is_none());
    }
}
