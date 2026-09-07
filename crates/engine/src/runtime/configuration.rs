use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use tracing::info;
use zero_config::{ModeConfig, RuntimeConfig};

use super::Engine;
use crate::{EngineError, EnginePlan};

impl Engine {
    /// Rebuild and atomically install the route/config plan, persist it when
    /// this engine owns a source path, then notify runtime subscribers.
    pub fn reload_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, true)?;
        self.commit_config_change();
        Ok(())
    }

    /// Rebuild and atomically install a runtime-only configuration overlay.
    ///
    /// This is the boundary for caller-owned ephemeral configuration: the
    /// effective runtime changes, but the operator-owned source file does not.
    /// A process restart therefore reconstructs only the persisted local
    /// deployment configuration.
    pub fn reload_runtime_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, false)?;
        self.commit_config_change();
        Ok(())
    }

    /// Install a persisted candidate without publishing a committed revision.
    /// The proxy transaction calls [`Self::commit_config_change`] only after
    /// listener and application-service reconciliation succeeds.
    pub fn stage_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, true)
    }

    /// Install an ephemeral candidate without publishing a committed revision.
    pub fn stage_runtime_config(&self, new_config: RuntimeConfig) -> Result<(), EngineError> {
        self.reload_config_inner(new_config, false)
    }

    /// Restore this engine's last-known-good snapshot after a staged apply
    /// fails. Rebuilding only its config would lose selections and disconnect
    /// the still-running old probe tasks from their policy state.
    pub fn restore_staged_snapshot(
        &self,
        snapshot: Arc<super::EngineRuntimeSnapshot>,
        persist: bool,
    ) -> Result<(), EngineError> {
        if snapshot.config_revision() != self.config_revision() {
            return Err(EngineError::InvalidPlan {
                message: "cannot restore a snapshot across a committed configuration change"
                    .to_owned(),
            });
        }
        if persist {
            if let Some(path) = &self.config_path {
                write_config_to_file(path, snapshot.config())?;
            }
        }
        *self.mode.lock().unwrap_or_else(|error| error.into_inner()) = snapshot.config.mode.clone();
        self.principal_policies
            .replace_from_config(snapshot.config());
        self.event_log
            .set_capacity(snapshot.config.runtime.event_log_capacity);
        *self
            .runtime_snapshot
            .write()
            .expect("runtime snapshot lock poisoned") = snapshot;
        self.passive_relay_health.clear();
        self.notify_reload();
        Ok(())
    }

    fn reload_config_inner(
        &self,
        new_config: RuntimeConfig,
        persist: bool,
    ) -> Result<(), EngineError> {
        let event_log_capacity = new_config.runtime.event_log_capacity;
        if self.config().runtime.principal_quota_state_path
            != new_config.runtime.principal_quota_state_path
        {
            return Err(EngineError::InvalidPlan {
                message: "runtime.principal_quota_state_path cannot change during live reload"
                    .to_owned(),
            });
        }
        let new_router = Arc::new(new_config.compile_route()?);
        let bypass = Arc::new(new_config.compile_route_bypass()?);
        let new_plan = Arc::new(EnginePlan::build(&new_config)?);
        if persist {
            if let Some(path) = &self.config_path {
                write_config_to_file(path, &new_config)?;
                info!(path = %path.display(), "config persisted");
            }
        }

        *self.mode.lock().unwrap_or_else(|error| error.into_inner()) = new_config.mode.clone();

        self.principal_policies.replace_from_config(&new_config);
        let mut current = self
            .runtime_snapshot
            .write()
            .expect("runtime snapshot lock poisoned");
        let outbound_group_state = crate::groups::OutboundGroupStateStore::for_plan(
            &new_plan,
            Some((&current.plan, &current.outbound_group_state)),
        );
        *current = Arc::new(super::EngineRuntimeSnapshot {
            config_revision: Arc::new(std::sync::atomic::AtomicU64::new(current.config_revision())),
            config: Arc::new(new_config),
            plan: new_plan,
            router: new_router,
            bypass,
            outbound_group_state,
        });
        drop(current);
        self.passive_relay_health.clear();
        self.event_log.set_capacity(event_log_capacity);
        self.notify_reload();
        Ok(())
    }

    fn notify_reload(&self) {
        for sender in self
            .reload_notify
            .lock()
            .expect("reload notify lock poisoned")
            .iter()
        {
            let _ = sender.send(());
        }
    }

    /// Publish the currently staged runtime snapshot as one committed
    /// configuration generation.
    pub fn commit_config_change(&self) -> u64 {
        let revision = self
            .config_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        let current = self.runtime_snapshot();
        current
            .config_revision
            .store(revision, std::sync::atomic::Ordering::Release);
        self.event_log.push_config_changed(revision);
        revision
    }

    pub(crate) fn commit_mode_change(&self, mode: ModeConfig) -> u64 {
        let revision = self
            .config_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        let current = self.runtime_snapshot();
        let mut config = (*current.config).clone();
        config.mode = mode;
        *self
            .runtime_snapshot
            .write()
            .expect("runtime snapshot lock poisoned") = Arc::new(super::EngineRuntimeSnapshot {
            config_revision: Arc::new(std::sync::atomic::AtomicU64::new(revision)),
            config: Arc::new(config),
            plan: current.plan.clone(),
            router: current.router.clone(),
            bypass: current.bypass.clone(),
            outbound_group_state: current.outbound_group_state.clone(),
        });
        self.event_log.push_config_changed(revision);
        revision
    }

    pub fn subscribe_reload(&self) -> std::sync::mpsc::Receiver<()> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.reload_notify
            .lock()
            .expect("reload notify lock poisoned")
            .push(sender);
        receiver
    }
}

fn write_config_to_file(path: &Path, config: &RuntimeConfig) -> Result<(), io::Error> {
    let json = serde_json::to_string_pretty(config).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serialize config: {error}"),
        )
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(json.as_bytes())?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
