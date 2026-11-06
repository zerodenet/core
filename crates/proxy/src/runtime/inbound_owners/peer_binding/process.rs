//! The one pre-admission metadata worker belongs to this actual peer/caller.
use super::*;
use zero_platform_tokio::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport};
impl HostPeerBinding {
    pub(crate) async fn lookup_local_process(
        &self,
        source: std::net::SocketAddr,
        transport: LocalProcessTransport,
        read_limit: std::num::NonZeroUsize,
    ) -> Result<Option<LocalProcessInfo>, ChannelError> {
        self.wait_bound().await?;
        let caller = self
            .join_witness()
            .ok_or_else(|| self.error(Reason::Revoked, "actual lookup caller is absent"))?;
        if caller.is_joined() || self.cancel.is_cancelled() {
            return Err(self.error(Reason::Revoked, "actual lookup caller is stopped"));
        }
        // Retired originals can be reclaimed only after native, join and ACK.
        // No temporary absence can release a previous Unknown controller.
        let previous = self.process.lock().unwrap().clone();
        let retired = previous.as_ref().is_none_or(|original| original.retired());
        let original = LocalProcessLookup::prepare(
            caller,
            source,
            transport,
            self.cancel.clone(),
            read_limit,
        )?;
        let removed = {
            let mut slot = self.process.lock().unwrap();
            if !retired
                || !match (&*slot, &previous) {
                    (Some(actual), Some(previous)) => Arc::ptr_eq(actual, previous),
                    (None, None) => true,
                    _ => false,
                }
            {
                let mut source = self.error(
                    Reason::Capacity,
                    "same caller retains its metadata original",
                );
                source.stage = Stage::Prepare;
                source.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
                return Err(source);
            }
            slot.replace(original.clone())
        };
        drop(removed);
        let start = original.register(&self.registrar)?;
        if self.cancel.is_cancelled() || self.observed_join().is_some() {
            return Err(self.error(
                Reason::Revoked,
                "actual caller stopped during lookup registration",
            ));
        }
        // Binding strongly held this original before registration. Its exact
        // witness was installed before releasing the existing blocking worker.
        start.start();
        original.result(&self.cancel).await
    }
    pub(super) fn observe_process_joined(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        let original = self.process.lock().unwrap().clone()?;
        let witness = original.witness()?;
        if witness.task_id() != Some(task) {
            return None;
        }
        let joined = witness.try_joined()?;
        // Unknown close remains in the same original/binding. Cleanup faults
        // are returned by close_process_lookup; no debug/drop handoff.
        if let Err(source) = original.collect_native_after_join() {
            self.process_cleanup_failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| Arc::new(source));
        }
        if joined.result().is_err() {
            self.process_failure
                .lock()
                .unwrap()
                .get_or_insert_with(|| joined.clone());
            // A real local receiver precedes acceptance. Host ACK remains the
            // original TaskScope consumer's independent operation.
            if witness.acknowledge_hosted_failure(&joined).is_err() {
                return None;
            }
        }
        Some(joined)
    }
    pub(crate) fn process_retired(&self) -> bool {
        let original = self.process.lock().unwrap().clone();
        original.as_ref().is_none_or(|original| original.retired())
    }
    pub(super) async fn close_process_lookup(&self, deadline: Instant) -> Result<(), ChannelError> {
        let original = self.process.lock().unwrap().clone();
        if let Some(original) = original {
            original.cancel();
            original.wait_retired(deadline).await?;
        }
        Ok(())
    }
}
