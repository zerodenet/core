//! Actual users and original driver waits, without a second owner.
use super::*;
impl OwnedEndpointOwner {
    /// Complete only actual sealed users and driver joins. This never closes
    /// or acknowledges the granting host's native main/probe originals.
    pub async fn shutdown_users(
        &self,
        deadline: Instant,
    ) -> Result<OwnedEndpointUsersReport, ChannelError> {
        self.begin_shutdown();
        self.shared.users.wait_empty(deadline).await?;
        let joined = self.drivers.wait_joined(deadline).await?;
        Ok(OwnedEndpointUsersReport {
            active_users: self.active_users(),
            drivers: self.drivers.task_status(),
            joined,
            observer_faulted: self.native.callbacks().failed(),
            suppressed_observer_panics: self.native.suppressed(),
        })
    }
    /// Original users/driver wait; a caller deadline only observes its owner.
    pub async fn shutdown_users_original(&self) -> Result<OwnedEndpointUsersReport, ChannelError> {
        self.begin_shutdown();
        self.shared.users.wait_empty_original().await?;
        let joined = self.drivers.wait_joined_original().await?;
        Ok(OwnedEndpointUsersReport {
            active_users: self.active_users(),
            drivers: self.drivers.task_status(),
            joined,
            observer_faulted: self.native.callbacks().failed(),
            suppressed_observer_panics: self.native.suppressed(),
        })
    }
}
