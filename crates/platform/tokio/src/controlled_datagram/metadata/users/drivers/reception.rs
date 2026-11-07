use super::*;
use tokio::time::{timeout_at, Instant};

/// Unique owning exit receiver. Mere task-status/body observation never calls
/// its reception operation or acknowledges a hosted local failure.
pub struct DriverExitReceiver<O: DriverKind, B: Send + Sync + 'static> {
    pub(super) shared: Arc<Shared<O, B>>,
}
/// The same actual joined receipt retained before acknowledging local delivery.
/// Its source is shared with the original host receipt; host ACK is separate.
pub struct DriverDelivery<B> {
    pub joined: TaskJoined,
    pub body: Option<Arc<B>>,
    pub(super) original: Arc<dyn std::any::Any + Send + Sync>,
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverExitReceiver<O, B> {
    /// Passive observation of the same received original registration. This
    /// consumes no exit and acknowledges neither local failure nor Host credit.
    pub async fn wait_registration_released(
        &self,
        id: TaskId,
        deadline: Instant,
    ) -> Result<TaskJoined, ChannelError> {
        let record = self
            .shared
            .state
            .lock()
            .unwrap()
            .records
            .get(&id)
            .cloned()
            .ok_or_else(|| ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component))?;
        if !record.delivered.load(Ordering::Acquire) {
            return Err(ChannelError::new(
                Stage::Close,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let witness = record
            .registration
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, witness)| witness.clone())
            .ok_or_else(|| ChannelError::new(Stage::Close, Reason::Internal, Impact::Component))?;
        let joined = timeout_at(deadline, witness.wait_registration_released())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        if !witness.matches_joined(&joined) || !joined.registration_released() {
            return Err(ChannelError::new(
                Stage::Close,
                Reason::Internal,
                Impact::Component,
            ));
        }
        Ok(joined)
    }
    /// Invoke when this real owning receiver accepts this driver's outcome.
    /// Deadline/cancellation keeps the original record and performs no ACK.
    pub async fn receive(
        &mut self,
        id: TaskId,
        deadline: Instant,
    ) -> Result<DriverDelivery<B>, ChannelError> {
        timeout_at(deadline, self.receive_original(id))
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?
    }
    /// Same owning reception, retained by the original cleanup operation.
    /// An observer timeout cannot cancel this actual outcome receiver.
    pub async fn receive_original(
        &mut self,
        id: TaskId,
    ) -> Result<DriverDelivery<B>, ChannelError> {
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
        let joined = witness.wait().await;
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
        let body = record.body.lock().unwrap().current();
        Ok(DriverDelivery {
            joined: received,
            body,
            original: record,
        })
    }
}

impl<B> Clone for DriverDelivery<B> {
    fn clone(&self) -> Self {
        Self {
            joined: self.joined.clone(),
            body: self.body.clone(),
            original: self.original.clone(),
        }
    }
}

impl<O: DriverKind, B: Send + Sync + 'static> DriverExitReceiver<O, B> {
    /// Wait for any actual original join, retaining its record across cancellation.
    /// No task or new registry is created; the real host still consumes JoinSet.
    pub async fn receive_next(&mut self) -> Result<DriverDelivery<B>, ChannelError> {
        use std::{
            future::{poll_fn, Future},
            task::Poll,
        };
        let shared = self.shared.clone();
        loop {
            let changed = shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let records = self
                .shared
                .state
                .lock()
                .unwrap()
                .records
                .iter()
                .filter(|(_, r)| !r.delivered.load(Ordering::Acquire))
                .map(|(id, r)| {
                    (
                        *id,
                        r.registration
                            .lock()
                            .unwrap()
                            .as_ref()
                            .map(|(_, w)| w.clone()),
                    )
                })
                .collect::<Vec<_>>();
            let mut joins = Vec::new();
            for (id, witness) in records {
                if let Some(witness) = witness {
                    joins.push(Box::pin(async move {
                        witness.wait().await;
                        id
                    }));
                }
            }
            let any_joins = !joins.is_empty();
            let waiting = poll_fn(|cx| {
                for join in &mut joins {
                    if let Poll::Ready(id) = join.as_mut().poll(cx) {
                        return Poll::Ready(Some(id));
                    }
                }
                Poll::Pending
            });
            let id = tokio::select! { id=waiting, if any_joins=>id, _=&mut changed=>None };
            if let Some(id) = id {
                return self
                    .receive(id, Instant::now() + std::time::Duration::from_secs(5))
                    .await;
            }
        }
    }
}

impl<B: fmt::Debug> fmt::Debug for DriverDelivery<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DriverDelivery")
            .field("joined", &self.joined)
            .field("body", &self.body)
            .finish_non_exhaustive()
    }
}

mod registration;
