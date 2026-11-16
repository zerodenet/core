//! Consumer return and actual driver reception precede mask and native close.
use super::*;
impl OwnedMkcp {
    pub async fn retire_protocol_original(&mut self) -> Result<(), ChannelError> {
        self.stop.cancel();
        self.graph.revoke();
        self.graph.wait_empty().await?;
        // Reliability no longer polls any decorator before decorators stop.
        if let Some(id) = self.driver {
            self.receive(id).await?;
        }
        self.masks
            .sort_by_key(|(_, ordinal, _)| std::cmp::Reverse(*ordinal));
        for index in 0..self.masks.len() {
            let id = self.masks[index].0;
            self.masks[index].2.cancel();
            self.receive(id).await?;
        }
        drop(self.socket.take());
        Ok(())
    }
    async fn receive(&mut self, id: TaskId) -> Result<(), ChannelError> {
        if let Some(delivery) = self
            .received
            .iter()
            .find(|delivery| delivery.joined.id() == id)
        {
            return self.receiver.release_body(delivery);
        }
        let delivery = self.receiver.receive_original(id).await?;
        // Store before accepting the output. The observer may be interrupted
        // after either step without losing the original error or receipt.
        self.received.push(delivery);
        self.receiver.release_body(self.received.last().unwrap())
    }
}
