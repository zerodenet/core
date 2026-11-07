//! Physical return remains independent of the same original driver error.
use super::*;
impl BorrowedPacketSegment {
    /// Request final sealing after the same endpoint's users/drivers returned.
    /// This creates no native/Scope receipt and does not retire the loan.
    pub fn stop_final(&self) {
        self.final_retirement.store(true, Ordering::Release);
        self.stop();
    }
    pub async fn retire_final(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<PacketQueues, ChannelError> {
        self.stop_final();
        self.retire(end).await
    }
    /// The host alone consumes and ACKs the actual Scope exit. Until then this
    /// owner and its original queues stay retained; body return is no proof.
    pub async fn retire(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<PacketQueues, ChannelError> {
        let returned = self.retire_outcome(end).await?;
        if let Some(source) = returned
            .joined
            .as_ref()
            .and_then(|joined| joined.result().err())
        {
            // Keep the original unique queues for the fixed library owner.
            // Returning Err never fabricates a second queue or loses custody.
            *self.queues.lock().unwrap() = returned.queues.0;
            return Err(ChannelError::retained(source));
        }
        Ok(returned.queues)
    }
    /// Same actual retirement, returning physical facts independently of its
    /// original driver error. The Kernel receiver retains report/TaskJoined
    /// before forwarding the error; original failure is never NativeUnknown.
    pub async fn retire_final_outcome(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<BorrowedPacketReturned, ChannelError> {
        self.stop_final();
        self.retire_outcome(end).await
    }
    pub async fn retire_outcome(
        &mut self,
        end: tokio::time::Instant,
    ) -> Result<BorrowedPacketReturned, ChannelError> {
        self.stop();
        drop(self.input.take());
        if let Some(driver) = &self.driver {
            if self.joined.is_none() {
                self.joined = Some(
                    tokio::time::timeout_at(end, driver.wait())
                        .await
                        .map_err(|_| error(Reason::Timeout))?,
                );
            }
            let joined = self.joined.as_ref().unwrap();
            if joined.result().is_err() {
                driver
                    .acknowledge_hosted_failure(joined)
                    .map_err(|source| error(Reason::StalePlan).with_source(source))?;
            }
            tokio::time::timeout_at(end, driver.wait_registration_released())
                .await
                .map_err(|_| error(Reason::Timeout))?;
        }
        let report = self.adoption.retire(end).await?;
        if !report.is_complete() {
            return Err(error(Reason::Timeout));
        }
        let mut channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        if self.final_retirement.load(Ordering::Acquire) {
            channels.outgoing.close();
        }
        Ok(BorrowedPacketReturned {
            queues: PacketQueues(Some(channels), self.identity.clone()),
            joined: self.joined.clone(),
            report,
        })
    }
}

impl BorrowedPacketReturned {
    pub fn into_parts(
        self,
    ) -> (
        PacketQueues,
        Option<TaskJoined>,
        zero_platform_tokio::contract::lifecycle::CloseReport,
    ) {
        (self.queues, self.joined, self.report)
    }
}
