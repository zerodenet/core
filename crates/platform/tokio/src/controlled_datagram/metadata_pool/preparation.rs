//! Reserve this exact original before registering its real establishing Aux.
//! The guard never grants I/O; open_candidate still requires the installed
//! actual witness and the original Host/candidate checks.
use super::*;
use std::sync::atomic::Ordering;

pub struct MetadataPoolPreparation {
    permit: Option<MetadataPoolPermit>,
}
impl MetadataPoolGrants {
    /// Atomically charge this same owner's and Root's original before spawn.
    /// Only the dynamic ancillary declaration uses this concrete setup path.
    pub fn prepare_original(
        &self,
        tasks: &TaskRegistrar,
    ) -> Result<MetadataPoolPreparation, ChannelError> {
        Ok(MetadataPoolPreparation {
            permit: Some(self.reserve_inner(None, Some(tasks), None).map_err(
                |(error, returned)| {
                    drop(returned);
                    error
                },
            )?),
        })
    }
    /// Same original pending reservation and actual Scope. The controller is
    /// Root-held before an actual initializer can be registered or execute IO.
    pub fn prepare_protocol_original(
        &self,
        tasks: &TaskRegistrar,
        control: crate::contract::lifecycle::BoxChannelControl,
    ) -> Result<
        (
            MetadataPoolPreparation,
            crate::controlled_carrier::CarrierProtocolRetirement,
        ),
        crate::controlled_carrier::RejectedOriginalProtocolControl,
    > {
        match self.reserve_inner(None, Some(tasks), Some(control)) {
            Ok(permit) => {
                let retirement = permit
                    .0
                    .protocol
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .expect("paid metadata phase installed before publication")
                    .handle();
                Ok((
                    MetadataPoolPreparation {
                        permit: Some(permit),
                    },
                    retirement,
                ))
            }
            Err((error, returned)) => {
                Err(crate::controlled_carrier::RejectedOriginalProtocolControl {
                    error,
                    control: returned.expect("refused metadata did not consume control"),
                })
            }
        }
    }
}
impl MetadataPoolPreparation {
    /// Durable exact original observation, never native or task permission.
    pub fn origin(&self) -> MetadataPoolOrigin {
        self.permit
            .as_ref()
            .expect("unique prepared original")
            .origin()
    }

    /// Register the actual establishing Aux only, in this original bound
    /// Scope and its existing task limit. The Root charge already exists.
    /// Actual IO still needs this returned permit, sealed candidate and Host.
    pub fn spawn_auxiliary<F>(
        mut self,
        tasks: &TaskRegistrar,
        task: F,
    ) -> Result<(MetadataPoolPermit, TaskJoinWitness), ChannelError>
    where
        F: std::future::Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let permit = self.permit.as_ref().expect("unique prepared original");
        let owner = permit
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata setup owner disappeared"))?;
        let authority = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| {
                error(
                    Reason::InvalidConfig,
                    "metadata setup has no actual Host binding",
                )
            })?;
        if !authority.scope.matches(tasks)
            || !authority.scope.registration_open()
            || authority.host_cancel.is_cancelled()
            || owner.cancel.is_cancelled()
        {
            return Err(error(
                Reason::Revoked,
                "metadata setup requires its actual active Scope",
            ));
        }
        // If registration unwinds before returning its witness, retain the
        // actual Scope census. Pending=None cannot prove no task was created.
        permit
            .0
            .registration_uncertain
            .store(true, Ordering::Release);
        let callbacks = tasks.defer_callbacks();
        let (admit, admitted) = tokio::sync::oneshot::channel::<()>();
        let registered =
            tasks.spawn_tracked(crate::contract::scope::TaskClass::Auxiliary, async move {
                if admitted.await.is_err() {
                    return Ok(());
                }
                task.await
            });
        let (_, caller) = match registered {
            Ok(actual) => actual,
            Err(source) => {
                // These typed registrar failures guarantee no actual task.
                permit
                    .0
                    .registration_uncertain
                    .store(false, Ordering::Release);
                let reason = match source {
                    crate::contract::scope::SpawnError::Closing => Reason::Revoked,
                    crate::contract::scope::SpawnError::NotStarted => Reason::InvalidConfig,
                    crate::contract::scope::SpawnError::Capacity
                    | crate::contract::scope::SpawnError::IdentityExhausted => Reason::Capacity,
                };
                return Err(error(reason, "original metadata Aux registration rejected")
                    .with_source(source));
            }
        };
        assert!(
            permit.0.caller.set(caller.clone()).is_ok(),
            "one actual metadata initializer"
        );
        permit
            .0
            .registration_uncertain
            .store(false, Ordering::Release);
        // Keep the real witness before this last fallible active/Host check.
        // Refusal drops the gate and retires the same original until real ACK.
        authority.check(&owner, &caller)?;
        permit.0.preparing.store(false, Ordering::Release);
        let permit = self.permit.take().expect("bound prepared original");
        let _ = admit.send(());
        drop(callbacks);
        Ok((permit, caller))
    }
}
impl Drop for MetadataPoolPreparation {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.0.preparing.store(false, Ordering::Release);
            permit.0.retired.cancel();
        }
    }
}
