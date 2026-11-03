//! Concrete configuration wake-up values; no task, I/O or runtime owner.
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct ConfigurationReconcileNotifications(watch::Sender<()>);
impl Default for ConfigurationReconcileNotifications {
    fn default() -> Self {
        Self(watch::channel(()).0)
    }
}
impl ConfigurationReconcileNotifications {
    pub fn publish(&self) {
        self.0.send_replace(());
    }
    pub fn subscribe(&self) -> ConfigurationReconcileChanges {
        ConfigurationReconcileChanges(self.0.subscribe())
    }
}

/// One caller's direct observation of the same domain notifications. This
/// value does not start an executor, acquire I/O, or add a relay worker.
#[derive(Debug)]
pub struct ConfigurationReconcileChanges(watch::Receiver<()>);
impl ConfigurationReconcileChanges {
    /// Uses the original watch receiver's cancellation-safe version tracking.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.0.changed().await
    }
    pub fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.0.has_changed()
    }
}
