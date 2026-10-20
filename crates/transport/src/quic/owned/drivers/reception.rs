use super::*;
use tokio::time::{timeout_at, Instant};

/// Unique owning exit receiver. Mere task-status/body observation never calls
/// its reception operation or acknowledges a hosted local failure.
pub struct DriverExitReceiver {
    pub(super) shared: Arc<Shared>,
}
/// The same actual joined receipt retained before acknowledging local delivery.
/// Its source is shared with the original host receipt; host ACK is separate.
#[derive(Clone, Debug)]
pub struct DriverDelivery {
    pub joined: TaskJoined,
    pub body: Option<Arc<DriverBody>>,
}
impl DriverExitReceiver {
    /// Invoke when this real owning receiver accepts this driver's outcome.
    /// Deadline/cancellation keeps the original record and performs no ACK.
    pub async fn receive(
        &mut self,
        id: TaskId,
        deadline: Instant,
    ) -> Result<DriverDelivery, ChannelError> {
        let record = self
            .shared
            .state
            .lock()
            .unwrap()
            .records
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                ChannelError::new(Stage::Close, Reason::InvalidConfig, Impact::Component)
            })?;
        let witness = record
            .registration
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, witness)| witness.clone())
            .ok_or_else(|| ChannelError::new(Stage::Close, Reason::Internal, Impact::Component))?;
        let joined = timeout_at(deadline, witness.wait())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        // Retain the exact native joined receipt and original error Arc before
        // telling the hosted owner that this receiver accepted that failure.
        let received = record
            .received
            .lock()
            .unwrap()
            .get_or_insert(joined)
            .clone();
        if received.result().is_err() {
            witness
                .acknowledge_hosted_failure(&received)
                .map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                        .with_source(source)
                })?;
        }
        record.delivered.store(true, Ordering::Release);
        let body = record.body.lock().unwrap().clone();
        Ok(DriverDelivery {
            joined: received,
            body,
        })
    }
}
