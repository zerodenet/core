//! Original Source quota waits, with no native or Task ACK authority.
use super::pool::acked;
use super::*;
impl Pool {
    /// Passive local/global admission wait. Existing original close/Scope
    /// collectors alone establish native/users closure and real driver joins.
    pub(super) async fn wait_capacity(&self) -> Result<(), ChannelError> {
        let root = self
            .originals
            .get()
            .map(|binding| {
                binding.root.upgrade().ok_or_else(|| {
                    failure(Stage::Prepare, Reason::Revoked, "original root retired")
                })
            })
            .transpose()?;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let closed = {
                let state = self.state();
                state
                    .entries
                    .values()
                    .filter(|entry| entry.state().phase == Phase::Closed)
                    .cloned()
                    .collect::<Vec<_>>()
            };
            for entry in &closed {
                self.confirm(entry);
            }
            let local = {
                let state = self.state();
                if self.cancel.is_cancelled() {
                    return Err(failure(
                        Stage::Prepare,
                        Reason::Revoked,
                        "accepted TCP admission revoked",
                    ));
                }
                for entry in state.entries.values() {
                    let status = entry.state();
                    if matches!(status.phase, Phase::Unknown | Phase::Acquiring) {
                        return Err(retained(
                            status
                                .unknown
                                .clone()
                                .unwrap_or_else(|| entry.uncertain.clone()),
                        ));
                    }
                }
                if state.next.checked_add(1).is_none() {
                    return Err(failure(
                        Stage::Prepare,
                        Reason::Capacity,
                        "accepted TCP identity exhausted",
                    ));
                }
                state.entries.len() < self.limit.get()
            };
            let waiting_users = closed
                .iter()
                .filter_map(|entry| entry.users.as_ref())
                .filter(|users| !users.complete())
                .cloned()
                .collect::<Vec<_>>();
            let users_changed = async {
                any_changed(waiting_users.into_iter().map(|users| async move {
                    users.wait_complete().await;
                }))
                .await;
            };
            let acks = closed
                .iter()
                .filter_map(|entry| entry.users.as_ref())
                .flat_map(|users| users.original_acks())
                .filter(|witness| !acked(witness))
                .collect::<Vec<_>>();
            let original_acks = async {
                any_changed(acks.into_iter().map(|witness| async move {
                    witness.wait_registration_released().await;
                }))
                .await;
            };
            if local {
                if let Some(receiver) = self.task_failures.get() {
                    tokio::select! { biased;
                        _ = self.cancel.cancelled() => return Err(failure(Stage::Prepare, Reason::Revoked, "accepted TCP admission revoked")),
                        _ = &mut changed => continue,
                        result = receiver.wait_capacity() => result?,
                    }
                }
                if let Some(root) = &root {
                    tokio::select! {biased;_=self.cancel.cancelled()=>return Err(failure(Stage::Prepare,Reason::Revoked,"accepted TCP admission revoked")),_=changed=>{},_=root.wait_capacity()=>return Ok(())}
                } else {
                    return Ok(());
                }
            } else {
                tokio::select! {biased;_=self.cancel.cancelled()=>return Err(failure(Stage::Prepare,Reason::Revoked,"accepted TCP admission revoked")),_=changed=>{},_=original_acks=>{},_=users_changed=>{}}
            }
        }
    }
}

// Poll only this finite snapshot of actual users/witnesses. An empty snapshot
// stays Pending alongside the original Pool notification/cancellation branches.
async fn any_changed<F: std::future::Future>(waiting: impl Iterator<Item = F>) {
    let mut waiting = waiting.map(Box::pin).collect::<Vec<_>>();
    std::future::poll_fn(|cx| {
        for original in &mut waiting {
            if original.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(());
            }
        }
        std::task::Poll::Pending
    })
    .await;
}
