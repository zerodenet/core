//! Independent joined children continue cleanup after a sibling reports failure.
use super::*;
impl InboundPeerTasks {
    /// Only the original scope's dispatched TaskExit makes a child eligible.
    /// Cancellation retains both actual owners and already consumed evidence.
    pub(crate) async fn retire_joined(&self, deadline: Instant) -> Result<(), ChannelError> {
        let joined = {
            let state = self.state.lock().expect("peer task state poisoned");
            state
                .active
                .iter()
                .filter(|(_, binding)| binding.joined_receipt().is_some())
                .map(|(task, binding)| (*task, binding.clone(), state.children.get(task).cloned()))
                .collect::<Vec<_>>()
        };
        let mut failure = None;
        for (task, binding, child) in joined {
            match binding.close_responses(deadline).await {
                Err(error) => {
                    failure.get_or_insert(error);
                    continue;
                }
                Ok(Some(source)) => {
                    self.fail(source.clone());
                    failure.get_or_insert_with(|| retained(source));
                    // A real panic must still allow the same owner's native
                    // cleanup after its original response Set has really joined.
                }
                Ok(None) => {}
            }
            if let Some(child) = child {
                if let Err(error) = self.retire_one(task, child, deadline).await {
                    failure.get_or_insert(error);
                }
            } else {
                let mut state = self.state.lock().expect("peer task state poisoned");
                if !state.claiming.contains(&task)
                    && !state.children.contains_key(&task)
                    && state
                        .active
                        .get(&task)
                        .is_some_and(|actual| Arc::ptr_eq(actual, &binding))
                {
                    state.active.remove(&task);
                    state.reclaimed = state.reclaimed.saturating_add(1);
                }
                drop(state);
                self.notify_changed();
            }
        }
        failure.map_or(Ok(()), Err)
    }
    async fn retire_one(
        &self,
        task: TaskId,
        child: Arc<PeerChild>,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        let mut owner = tokio::time::timeout_at(deadline, child.owner.lock())
            .await
            .map_err(|source| {
                error(Reason::Timeout, "peer resource cleanup is busy").with_source(source)
            })?;
        // A concurrent cleanup may have consumed this same confirmed owner.
        if !self
            .state
            .lock()
            .expect("peer task state poisoned")
            .children
            .get(&task)
            .is_some_and(|actual| Arc::ptr_eq(actual, &child))
        {
            return Ok(());
        }
        owner.revoke(CloseReason::Shutdown);
        let mut report = match owner.shutdown(deadline).await {
            Ok(report) => report,
            Err(failure) => {
                let failure = child.remember(failure);
                self.fail(failure.clone());
                return Err(retained(failure));
            }
        };
        let complete = report.is_complete();
        let mut failed = None;
        let mut receipts = child
            .receipts
            .lock()
            .expect("peer resource receipts poisoned");
        for exit in report.failed_tasks.drain(..) {
            let original = Arc::new(exit.error);
            if !receipts
                .failed_tasks
                .iter()
                .any(|saved| saved.id == exit.id)
            {
                receipts.failed_tasks.push(TaskFailure {
                    id: exit.id,
                    class: exit.class,
                    error: retained(original.clone()),
                });
            }
            failed.get_or_insert(original);
        }
        // Persist one-shot evidence before returning a timeout/failure.
        // Only the latest unfinished view replaces the previous view.
        receipts.unfinished.clear();
        receipts.append(report);
        if let Some(failure) = failed {
            drop(receipts);
            self.fail(failure.clone());
            return Err(retained(failure));
        }
        if !complete {
            return Err(error(
                Reason::Timeout,
                "peer native resource remains unconfirmed",
            ));
        }
        let closed_io_receipts = receipts.closed_io.len();
        drop(receipts);
        let mut state = self.state.lock().expect("peer task state poisoned");
        state.confirmed_children = state.confirmed_children.saturating_add(1);
        state.closed_io_receipts = state.closed_io_receipts.saturating_add(closed_io_receipts);
        // Successful evidence has been consumed here. Retain bounded
        // totals, not one historical string/owner per completed peer.
        state.children.remove(&task);
        state.active.remove(&task);
        state.reclaimed = state.reclaimed.saturating_add(1);
        drop(state);
        self.notify_changed();
        Ok(())
    }
}
