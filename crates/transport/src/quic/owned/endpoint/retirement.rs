//! Actual users and original driver waits, without a second owner.
use super::*;
impl OwnedEndpointOwner {
    /// Same sealed native users/driver wait, with bodies retained in the original
    /// ledger. The actual receiver must independently accept each delivery.
    pub async fn wait_users_original(&self) -> Result<(), ChannelError> {
        self.begin_shutdown();
        self.shared.users.wait_empty_original().await
    }
    /// Observe the exact original ledger in consumer-before-mask order without
    /// allocating a task snapshot. The original mask ordinal fixes drain order.
    pub fn next_shutdown_driver(
        &self,
        previous: Option<zero_core::component::TaskId>,
    ) -> Option<zero_core::component::TaskId> {
        use std::cmp::Reverse;
        let key = |id| match self.drivers.origin(id) {
            Some(super::super::OwnedDriverOrigin::Mask { ordinal, .. }) => {
                (1, Reverse(ordinal), id)
            }
            _ => (0, Reverse(0), id),
        };
        let previous = previous.map(key);
        let mut cursor = None;
        let mut selected = None;
        while let Some(id) = self.drivers.next_task(cursor) {
            cursor = Some(id);
            let candidate = key(id);
            if previous.is_none_or(|previous| candidate > previous)
                && selected.is_none_or(|current| candidate < key(current))
            {
                selected = Some(id);
            }
        }
        selected
    }
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
