//! Attach one concrete Endpoint to this exact pre-I/O ancillary original.
use super::*;
use crate::controlled_carrier::{CarrierProtocolRetirement, RejectedOriginalProtocolControl};
impl MetadataPoolPermit {
    pub fn install_protocol_control(
        &self,
        tasks: &TaskRegistrar,
        actor: &TaskJoinWitness,
        control: crate::contract::lifecycle::BoxChannelControl,
    ) -> Result<CarrierProtocolRetirement, RejectedOriginalProtocolControl> {
        let installed = (|| {
            let owner = self
                .0
                .owner
                .upgrade()
                .ok_or_else(|| error(Reason::Revoked, "metadata setup owner disappeared"))?;
            let bound = owner
                .state
                .lock()
                .expect("metadata pool poisoned")
                .bound
                .clone()
                .ok_or_else(|| {
                    error(Reason::InvalidConfig, "metadata setup has no Host binding")
                })?;
            if !matches!(
                &bound.declaration,
                ResourceDeclaration::ControlledMetadataCarrier { .. }
            ) || !bound.scope.matches(tasks)
                || !self
                    .0
                    .caller
                    .get()
                    .is_some_and(|actual| actual.same_registration(actor))
                || self.0.preparing.load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(error(
                    Reason::StalePlan,
                    "foreign metadata protocol owner or actor",
                ));
            }
            bound.check(&owner, actor)?;
            let native = self.0.native()?;
            if native.attempted
                || native.transferred
                || self.0.closed.get().is_some()
                || self.0.retired.is_cancelled()
            {
                return Err(error(
                    Reason::Revoked,
                    "metadata protocol must be retained before first I/O",
                ));
            }
            let actual = self
                .0
                .protocol
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if actual.is_some() {
                return Err(error(
                    Reason::StalePlan,
                    "metadata protocol already installed",
                ));
            }
            // Return the still-empty install gate, before consuming the unique control.
            Ok((actual, native))
        })();
        let (mut installed, native) = match installed {
            Ok(installed) => installed,
            Err(error) => return Err(RejectedOriginalProtocolControl { control, error }),
        };
        let phase =
            crate::original_protocol::Phase::new(control, self.0.instance, self.0.root_bound);
        let handle = phase.handle();
        let once = self.0.protocol_stop.set(phase.cancellation());
        assert!(once.is_ok(), "one metadata protocol per exact original");
        *installed = Some(phase);
        drop(installed);
        drop(native);
        let wake = self
            .0
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(handle)
    }
    /// Drive the same installed phase for an explicit original owner shutdown.
    /// Native closure and actual task ACKs remain separate. No new controller,
    /// worker, task consumer or alternate cleanup deadline is created here.
    pub async fn retire_protocol(
        &self,
        tasks: &TaskRegistrar,
        end: tokio::time::Instant,
    ) -> Result<bool, ChannelError> {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "metadata owner disappeared"))?;
        let bound = owner
            .state
            .lock()
            .expect("metadata pool poisoned")
            .bound
            .clone()
            .ok_or_else(|| {
                error(
                    Reason::InvalidConfig,
                    "metadata protocol has no bound owner",
                )
            })?;
        if !bound.scope.matches(tasks) {
            return Err(error(
                Reason::StalePlan,
                "foreign metadata protocol close scope",
            ));
        }
        let phase = self
            .0
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Ok(match phase {
            Some(phase) => phase.wait(end).await,
            None => true,
        })
    }
    /// Requests the original phase/native cleanup only; no close receipt.
    pub fn request_retirement(&self) {
        let phase = self
            .0
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.0.retired.cancel();
        if let Some(phase) = phase {
            phase.request();
        }
    }
}
