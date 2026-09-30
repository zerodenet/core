use tokio::sync::watch;

/// Created before spawning so abort-before-first-poll also confirms completion.
pub(super) struct Completion(pub watch::Sender<bool>);

impl Drop for Completion {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl super::SharedRawIpDevice {
    pub(crate) async fn wait_stopped(&self) {
        let mut completed = self.completed.clone();
        while !*completed.borrow_and_update() {
            if completed.changed().await.is_err() {
                break;
            }
        }
    }
}
