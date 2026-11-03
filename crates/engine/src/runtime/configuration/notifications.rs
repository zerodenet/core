//! Domain notifications, directly borrowed by the same host without relay tasks.
impl super::super::Engine {
    /// Wake the existing host reconciler without exposing its pending candidate
    /// through the committed control-plane snapshot.
    pub fn notify_configuration_reconcile(&self) {
        self.notify_reload();
    }

    pub(super) fn notify_reload(&self) {
        // Store the notification even if no async borrower is currently
        // polling. The host consumes current domain/pending-operation state,
        // so coalescing wake-ups never changes the original ACK identity.
        self.reload_changes.publish();
        for sender in self
            .reload_notify
            .lock()
            .expect("reload notify lock poisoned")
            .iter()
        {
            let _ = sender.send(());
        }
    }

    /// Subscribe before capturing the current domain snapshot. Changes remain
    /// pending across startup awaits or cancellation of `Receiver::changed`.
    /// No relay task or additional runtime owner is created.
    pub fn subscribe_configuration_reconcile(
        &self,
    ) -> zero_runtime::configuration::ConfigurationReconcileChanges {
        self.reload_changes.subscribe()
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
