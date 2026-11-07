//! Actual protocol rejection and one failed handoff remain distinct typed causes.
use super::*;
#[derive(Debug)]
struct InstallFailure {
    rejected: Arc<ChannelError>,
    install: Arc<ChannelError>,
}
impl std::fmt::Display for InstallFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (preparation job handoff failed: {})",
            self.rejected, self.install
        )
    }
}
impl std::error::Error for InstallFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.rejected.as_ref())
    }
}
fn with_install_source(rejected: Arc<ChannelError>, install: Arc<ChannelError>) -> ChannelError {
    ChannelError::retained(rejected.clone()).with_source(InstallFailure { rejected, install })
}
impl CarrierPreparationJob {
    /// Construct diagnostic metadata for a failed owner handoff, including one
    /// rejected before a job key could be taken. This grants no authority and
    /// acknowledges no error or cleanup. Both actual typed causes stay owned.
    pub fn failed_install_error(
        rejected: Arc<ChannelError>,
        install: ChannelError,
    ) -> ChannelError {
        with_install_source(rejected, Arc::new(install))
    }
    /// The exact rejected outcome was retained by the actual waiting record
    /// before it was acknowledged. Keep the actual protocol error and original
    /// Scope receipt separately: the real runner may wrap the first Arc. Neither
    /// an error source chain nor its text grants this custody transition.
    pub fn accept_rejection(
        &self,
        source: Arc<ChannelError>,
        received: &TaskJoined,
    ) -> Result<(), ChannelError> {
        let slot = self.slot.upgrade().ok_or_else(revoked)?;
        let fail = || {
            error(
                slot.instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            )
        };
        let auxiliary = self.preparation.auxiliary.get().ok_or_else(fail)?;
        let joined = auxiliary.try_joined().ok_or_else(fail)?;
        if !auxiliary.matches_joined(&joined)
            || !joined.registration_released()
            || self.preparation.published.load(Ordering::Acquire)
            || !slot.state().auxiliary_accepted
            || !auxiliary.matches_joined(received)
            || joined.result().is_ok()
            || !received.registration_released()
            || !received.failure_accepted()
        {
            return Err(fail());
        }
        let mut failure = self.job.failure.lock().unwrap();
        if failure
            .as_ref()
            .is_some_and(|(actual, _)| !Arc::ptr_eq(actual, &source))
        {
            return Err(fail());
        }
        failure.get_or_insert((source, received.clone()));
        Ok(())
    }

    /// Metadata only, not failure ACK, native disposition or handoff authority.
    /// One actual installation attempt has one secondary cause. Keep its exact
    /// Arc on this same original until actual original retirement, and on the
    /// error delivered to the real caller. Retry policy remains the rejection's.
    pub fn retain_failed_install(
        &self,
        rejected: Arc<ChannelError>,
        install: ChannelError,
    ) -> ChannelError {
        let install = self
            .job
            .install_failure
            .get_or_init(|| Arc::new(install))
            .clone();
        with_install_source(rejected, install)
    }
}
