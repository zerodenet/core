//! One persistent restart intent on the existing TUN control, never a device owner.
use super::*;
use std::sync::{Mutex, Weak};

/// Retains the old exact receipts and complete immutable creation/route input.
/// No native IO, Scope or controller is created by this recovery metadata.
#[derive(Debug)]
pub(crate) struct TunBudgetRecovery {
    original_id: u64,
    original: Arc<completion::TunCompletion>,
    restart: Arc<managed::Start>,
    resumed: Mutex<Option<Resumed>>,
}
#[derive(Debug)]
struct Resumed {
    config: Weak<zero_config::RuntimeConfig>,
    id: u64,
    removed: bool,
}
impl Proxy {
    /// Pure preflight before any actual Source StopOld. Presence is an owned
    /// restart responsibility, never evidence that native/Scope work is closed.
    pub(crate) fn validate_tun_packet_budget(
        &self,
        next: &zero_config::RuntimeConfig,
    ) -> Result<bool, EngineError> {
        let retained = self.tun_control.lock().unwrap().as_ref().map(|control| {
            (
                control.budget_recovery.clone(),
                control.completion.restart(),
            )
        });
        let Some((recovery, restart)) = retained else {
            return Ok(false);
        };
        let restart = recovery
            .as_ref()
            .map(|recovery| recovery.restart.clone())
            .or(restart)
            .ok_or_else(|| {
                io::Error::other("actual System TUN has no retained validated restart intent")
            })?;
        if restart.managed_config.is_none() && next.runtime.tun.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "cannot enable configured TUN while a command-managed TUN is running",
            )
            .into());
        }
        Ok(true)
    }
    pub(crate) fn has_tun_packet_budget_recovery(&self) -> bool {
        self.tun_control
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|control| control.budget_recovery.is_some())
    }
    /// Caller retains the actual Source idle/admission fences. Persist intent
    /// first, then wait for this same Source/routes/native/SDK and real joins.
    /// A failed/cancelled waiter leaves the original control and capsule intact.
    pub(crate) async fn stop_tun_for_packet_budget(
        &self,
        next: &zero_config::RuntimeConfig,
    ) -> Result<(), EngineError> {
        let _operation = self.tun_operation_lock.lock().await;
        let Some((id, completion, previous)) =
            self.tun_control.lock().unwrap().as_ref().map(|control| {
                (
                    control.id,
                    control.completion.clone(),
                    control.budget_recovery.clone(),
                )
            })
        else {
            return Ok(());
        };
        let recovery = match previous {
            Some(recovery) => recovery,
            None => Arc::new(TunBudgetRecovery {
                original_id: id,
                restart: completion.restart().ok_or_else(|| {
                    io::Error::other("actual System TUN has no retained validated restart intent")
                })?,
                original: completion.clone(),
                resumed: Mutex::new(None),
            }),
        };
        recovery.validate_desired(next.runtime.tun.as_ref())?;
        {
            let mut control = self.tun_control.lock().unwrap();
            let control = control
                .as_mut()
                .filter(|control| control.id == id && Arc::ptr_eq(&control.completion, &completion))
                .ok_or_else(|| {
                    io::Error::other("System TUN control changed during budget preparation")
                })?;
            if control
                .budget_recovery
                .as_ref()
                .is_some_and(|actual| !Arc::ptr_eq(actual, &recovery))
            {
                return Err(io::Error::other("System TUN retains another budget recovery").into());
            }
            control.budget_recovery = Some(recovery);
        }
        // Seal this actual Source synchronously before the first cancellable
        // await. A dropped idle fence must not reopen the stopped incarnation.
        completion.request_source_stop();
        if completion.resources_complete().await {
            return Ok(());
        }
        self.stop_tun_locked(true, true).await
    }
    /// Root policy must already be installed for this exact frozen candidate.
    /// Reuse the existing ordinary Source assembly; no second Host or Scope.
    pub(crate) async fn resume_tun_after_packet_budget(
        &self,
        config: Arc<zero_config::RuntimeConfig>,
    ) -> Result<(), EngineError> {
        let _operation = self.tun_operation_lock.lock().await;
        self.resume_tun_budget_locked(config).await
    }
    /// Commit confirmation only discards matching recovery metadata. It never
    /// supplies a native close or a Scope/user receipt for either incarnation.
    pub(crate) fn confirm_tun_packet_budget(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
    ) -> Result<(), EngineError> {
        let mut controls = self.tun_control.lock().unwrap();
        let Some(control) = controls.as_mut() else {
            return Ok(());
        };
        let Some(recovery) = control.budget_recovery.clone() else {
            return Ok(());
        };
        let removed = {
            let resumed = recovery.resumed.lock().unwrap();
            let resumed = resumed
                .as_ref()
                .filter(|resumed| {
                    resumed.id == control.id
                        && resumed
                            .config
                            .upgrade()
                            .is_some_and(|actual| Arc::ptr_eq(&actual, config))
                })
                .ok_or_else(|| {
                    io::Error::other(
                        "System TUN budget recovery was not completed for this exact candidate",
                    )
                })?;
            if !resumed.removed
                && (!control.completion.source_running()
                    || *control.shutdown.borrow()
                    || !self
                        .tun_info
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|info| info.id == control.id))
            {
                return Err(io::Error::other(
                    "restored System TUN Source stopped before commit confirmation",
                )
                .into());
            }
            resumed.removed
        };
        control.budget_recovery = None;
        // Removal was recorded only after original matching physical cleanup.
        let retired_control = if removed { controls.take() } else { None };
        drop(controls);
        drop(retired_control);
        drop(recovery);
        Ok(())
    }
}
impl TunBudgetRecovery {
    fn validate_desired(
        &self,
        desired: Option<&zero_config::TunConfig>,
    ) -> Result<(), EngineError> {
        if self.restart.managed_config.is_none() && desired.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "cannot enable configured TUN while a command-managed TUN is running",
            )
            .into());
        }
        Ok(())
    }
    fn record_resumed(&self, config: &Arc<zero_config::RuntimeConfig>, id: u64, removed: bool) {
        *self.resumed.lock().unwrap() = Some(Resumed {
            config: Arc::downgrade(config),
            id,
            removed,
        });
    }
}

mod resume;

#[cfg(test)]
mod tests;
