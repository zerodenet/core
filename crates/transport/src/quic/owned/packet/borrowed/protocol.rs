//! Return the unique already-accepted queue/view before its Slot closes.
use super::*;
use zero_platform_tokio::controlled_carrier::loan::CarrierEndpointUsersClosed;
/// This proves only actual queue return and the original driver outcome.
/// It carries neither NativeClosed nor Host registration ACK.
pub struct BorrowedProtocolReturned {
    pub queues: PacketQueues,
    pub joined: Option<TaskJoined>,
}
impl BorrowedPacketSegment {
    /// Called only by the same Slot's protocol phase after the endpoint's exact
    /// real user/driver graph was received. Ordinary retire remains unchanged.
    pub async fn return_protocol_queues_original(
        &mut self,
        receipt: CarrierEndpointUsersClosed,
    ) -> Result<BorrowedProtocolReturned, ChannelError> {
        if !receipt.matches(&self.adoption) {
            return Err(error(Reason::StalePlan));
        }
        self.adoption.retain_endpoint_users_closed(receipt)?;
        self.stop_final();
        drop(self.input.take());
        if let Some(driver) = &self.driver {
            if self.joined.is_none() {
                self.joined = Some(driver.wait().await);
            }
            let joined = self.joined.as_ref().expect("received actual driver");
            if !driver.matches_joined(joined) {
                return Err(error(Reason::StalePlan));
            }
            if joined.result().is_err() {
                driver
                    .acknowledge_hosted_failure(joined)
                    .map_err(|source| error(Reason::StalePlan).with_source(source))?;
            }
        }
        // Unique IO/view has returned from that exact future. The Root outside
        // this Phase still drives original LoanOwner/native and all real ACKs.
        let mut channels = self
            .queues
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| error(Reason::Revoked))?;
        channels.outgoing.close();
        Ok(BorrowedProtocolReturned {
            queues: PacketQueues(Some(channels), self.identity.clone()),
            joined: self.joined.clone(),
        })
    }
}
