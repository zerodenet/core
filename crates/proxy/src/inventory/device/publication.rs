//! One retained actual device publication, shared by the existing inventory.
use super::super::ProtocolInventory;
use crate::adapters::devices::PreparedDevices;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use zero_config::RuntimeConfig;
use zero_engine::EngineError;
pub(in crate::inventory) type PublicationSlot = Arc<Mutex<Option<Arc<Publication>>>>;
pub(in crate::inventory) struct Publication {
    config: Weak<RuntimeConfig>,
    prepared: tokio::sync::Mutex<Option<PreparedDevices>>,
    committed: AtomicBool,
}
impl std::fmt::Debug for Publication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePublication")
            .field("committed", &self.committed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
impl Publication {
    fn matches(&self, config: &Arc<RuntimeConfig>) -> bool {
        self.config.ptr_eq(&Arc::downgrade(config))
    }
}
impl ProtocolInventory {
    pub(crate) fn stage_device_publication(
        &self,
        config: &Arc<RuntimeConfig>,
        prepared: PreparedDevices,
    ) -> Result<(), EngineError> {
        let mut current = self
            .device_publication
            .lock()
            .expect("device publication poisoned");
        if current.is_some() {
            return Err(std::io::Error::other(
                "an actual device publication still owns this transition",
            )
            .into());
        }
        *current = Some(Arc::new(Publication {
            config: Arc::downgrade(config),
            prepared: tokio::sync::Mutex::new(Some(prepared)),
            committed: AtomicBool::new(false),
        }));
        Ok(())
    }
    pub(in crate::inventory) fn commit_device_publication<T>(
        &self,
        config: &Arc<RuntimeConfig>,
        commit: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let publication = self
            .device_publication
            .lock()
            .expect("device publication poisoned")
            .clone();
        let Some(publication) = publication else {
            return self.host.components().commit(config, commit);
        };
        if !publication.matches(config) {
            return Err("another exact device publication remains owned".into());
        }
        let mut prepared = publication
            .prepared
            .try_lock()
            .map_err(|_| "actual device publication is still being collected".to_owned())?;
        let prepared = prepared
            .as_mut()
            .ok_or_else(|| "device candidate has been stopped before commit".to_owned())?;
        // No await or caller-owned temporary separates catalog success from
        // confirmation of this very guard. The slot retains it across Ready ACK.
        let result = self.host.components().commit(config, || {
            let result = commit()?;
            prepared.publish_sync();
            Ok(result)
        })?;
        prepared.confirm_publication();
        publication.committed.store(true, Ordering::Release);
        Ok(result)
    }
    pub(crate) async fn finish_device_publication(
        &self,
        config: &Arc<RuntimeConfig>,
    ) -> Result<(), EngineError> {
        let publication = self
            .device_publication
            .lock()
            .expect("device publication poisoned")
            .clone();
        let Some(publication) = publication else {
            return Ok(());
        };
        if !publication.matches(config) || !publication.committed.load(Ordering::Acquire) {
            return Err(
                std::io::Error::other("device publication has no matching actual commit").into(),
            );
        }
        {
            let mut prepared = publication.prepared.lock().await;
            if let Some(prepared) = prepared.as_mut() {
                prepared.finish_publication().await;
            }
            prepared.take();
        }
        self.clear_device_publication(&publication);
        Ok(())
    }
    pub(in crate::inventory) async fn stop_device_publication(
        &self,
        config: Option<&Arc<RuntimeConfig>>,
        deadline: tokio::time::Instant,
    ) -> Result<Option<Arc<Publication>>, EngineError> {
        let publication = self
            .device_publication
            .lock()
            .expect("device publication poisoned")
            .clone();
        let Some(publication) = publication else {
            return Ok(None);
        };
        if config.is_some_and(|config| !publication.matches(config)) {
            return Err(
                std::io::Error::other("device cleanup names a different actual candidate").into(),
            );
        }
        {
            let mut prepared = tokio::time::timeout_at(deadline, publication.prepared.lock())
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "original device publication remains retained",
                    )
                })?;
            // Drop requests stop for fresh uncommitted originals only. The
            // catalog and device owners still owe all actual joins and closes.
            prepared.take();
        }
        Ok(Some(publication))
    }
    pub(in crate::inventory) fn clear_device_publication(&self, original: &Arc<Publication>) {
        let mut current = self
            .device_publication
            .lock()
            .expect("device publication poisoned");
        // Same config can name a later network generation. Only this actual
        // guard identity may clear its slot, never a late config-only callback.
        if current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, original))
        {
            current.take();
        }
    }
}

impl ProtocolInventory {
    /// Compensation retires the actual retained candidate, rather than naming
    /// its different last-good target as if it owned that candidate's guard.
    pub(crate) async fn discard_pending_device_candidates(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), EngineError> {
        let publication = self
            .device_publication
            .lock()
            .expect("device publication poisoned")
            .clone();
        let mut configs = Vec::new();
        if let Some(publication) = publication {
            let config = publication.config.upgrade().ok_or_else(|| {
                std::io::Error::other("actual device publication lost its retained catalog input")
            })?;
            if publication.committed.load(Ordering::Acquire) {
                self.finish_device_publication(&config).await?;
            } else {
                configs.push(config);
            }
        }
        for config in self.host.components().pending_candidates() {
            if !configs.iter().any(|actual| Arc::ptr_eq(actual, &config)) {
                configs.push(config);
            }
        }
        for config in configs {
            self.discard_device_components(&config, deadline).await?;
        }
        Ok(())
    }
}
