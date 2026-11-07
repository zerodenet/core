use super::*;
impl PacketSegment {
    pub fn stop(&self) {
        self.stop.cancel();
        self.graph.users().cancellation().cancel();
        self.graph.drivers().revoke_registration();
    }
    pub async fn retire(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<(PacketQueues, MetadataUsersClosedReceipt), ChannelError> {
        self.stop();
        drop(self.source.take());
        if let Some(id) = self.driver {
            if self.received.is_none() {
                self.received = Some(self.receiver.receive(id, end).await?);
            }
        }
        let completion = self.graph.completion();
        let receipt = if self.identity.endpoint_bound() {
            // Actual endpoint binding is installed before Kernel IO/driver
            // start. The same receiver persists the sole original output.
            if let Some(delivery) = self.received.as_ref() {
                self.receiver.release_body(delivery)?;
            }
            completion.try_owned_closed()
        } else {
            // Explicit independent PacketSegment library duty, no bound endpoint.
            completion.try_closed()
        }
        .ok_or_else(|| error(Reason::Timeout))?;
        let channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        if let Some(native) = &channels.native {
            native.lock().unwrap().current = None;
        }
        Ok((PacketQueues(Some(channels), self.identity.clone()), receipt))
    }
    /// Final retirement after the same endpoint's users and actual Quinn
    /// drivers joined. Unlike a live UDP hop, this seals its existing FIFO.
    /// A deadline retains the original worker and its pending native send.
    pub async fn retire_final(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<(PacketQueues, MetadataUsersClosedReceipt), ChannelError> {
        self.final_retirement.store(true, Ordering::Release);
        let (queues, receipt) = self.retire(end).await?;
        match self.identity.final_receipt(receipt) {
            Ok(receipt) => Ok((queues, receipt)),
            Err(receipt) => {
                // Keep the same unique returned queues across a cancelled or
                // incomplete final ledger reception, never turn it into Drop.
                *self.queues.lock().unwrap() = queues.0;
                let _ = receipt;
                Err(error(Reason::Timeout))
            }
        }
    }
}
