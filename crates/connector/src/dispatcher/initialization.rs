use super::*;
pub(super) struct InitSignal {
    sender: Option<mpsc::SyncSender<ConnectorResult<bool>>>,
    changed: Arc<tokio::sync::Notify>,
}
impl InitSignal {
    pub fn new(
        sender: mpsc::SyncSender<ConnectorResult<bool>>,
        changed: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            sender: Some(sender),
            changed,
        }
    }
    pub fn send(
        &self,
        value: ConnectorResult<bool>,
    ) -> Result<(), mpsc::SendError<ConnectorResult<bool>>> {
        let result = self
            .sender
            .as_ref()
            .expect("original readiness sender")
            .send(value);
        self.changed.notify_one();
        result
    }
}
impl Drop for InitSignal {
    fn drop(&mut self) {
        self.sender.take();
        self.changed.notify_one();
    }
}
