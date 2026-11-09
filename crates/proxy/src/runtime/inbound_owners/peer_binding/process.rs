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
        let receiver = self
            .task_failure
            .get()
            .ok_or_else(|| {
                self.error(
                    Reason::Revoked,
                    "metadata caller has no paid task-fault receiver",
                )
            })?
            .receiver();
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
        let root = self
            .lookup_root
            .upgrade()
            .ok_or_else(|| self.error(Reason::Revoked, "actual lookup root retired"))?;
        original.bind_original_totals(self.parent, &self.registrar, &root)?;
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
        drop(previous);
        let old_fault = self.process_task_failure.lock().unwrap().take();
        drop(old_fault);
        let _ = root.snapshot(); // Reconcile only already-proven original settlement.
        let mut fault = match receiver.reserve() {
            Ok(fault) => fault,
            Err(source) => {
                // No Root/task/native acquisition has run for this preparation.
                let removed = {
                    let mut slot = self.process.lock().unwrap();
                    if slot
                        .as_ref()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &original))
                    {
                        slot.take()
                    } else {
                        None
                    }
                };
                drop(removed);
                return Err(source);
            }
        };
        fault.begin_registration();
        let start = match original.register(&self.registrar) {
            Ok(start) => start,
            Err(source) => {
                fault.known_not_registered();
                return Err(source);
            }
        };
        let witness = original
            .witness()
            .expect("registered metadata worker witness");
        let registration = fault.registered(&witness)?;
        *self.process_task_failure.lock().unwrap() = Some(registration);
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
    pub(in crate::runtime::inbound_owners) fn observe_process_joined(
        &self,
        task: TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        let original = self.process.lock().unwrap().clone()?;
        let witness = original.witness()?;
        if witness.task_id() != Some(task) {
            return None;
        }
        let joined = witness.try_joined()?;
        let fault = self.process_task_failure.lock().unwrap().clone()?;
        if !fault.matches(&witness) || fault.receive(&joined).is_err() {
            return None;
        }
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
    pub(in crate::runtime::inbound_owners) fn receive_process_exit(
        &self,
        scope: &zero_platform_tokio::contract::scope::TaskScope,
        exit: &zero_platform_tokio::contract::scope::TaskExit,
    ) -> Option<Result<(), zero_platform_tokio::contract::scope::HostedReceiptError>> {
        let original = self.process.lock().unwrap().clone()?;
        let witness = original.witness()?;
        if witness.task_id() != Some(exit.id) {
            return None;
        }
        // observe_process_joined persists the same actual receipt in the paid
        // Source book before this unique Scope owner accepts its TaskExit.
        let joined = self.observe_process_joined(exit.id)?;
        Some(match &exit.result {
            Err(source) => scope.accept_retained_failure(&witness, &joined, source),
            Ok(()) => Ok(()),
        })
    }
    pub(crate) fn process_retired(&self) -> bool {
        let original = self.process.lock().unwrap().clone();
        original.as_ref().is_none_or(|original| original.retired())
    }
    #[cfg(test)]
    pub(in crate::runtime::inbound_owners) fn original_process_retired_for_test(&self) -> bool {
        let original = self.process.lock().unwrap().clone();
        let Some(original) = original else {
            return false;
        };
        let Some(witness) = original.witness() else {
            return false;
        };
        let Some(joined) = witness.try_joined() else {
            return false;
        };
        assert!(witness.matches_joined(&joined));
        assert!(witness
            .scope_identity()
            .same_scope(&self.registrar.scope_identity()));
        assert_eq!(
            joined.class(),
            zero_platform_tokio::contract::scope::TaskClass::Auxiliary
        );
        assert_ne!(Some(joined.id()), self.join_witness().unwrap().task_id());
        joined.result().unwrap();
        joined.registration_released() && original.retired()
    }
    pub(in crate::runtime::inbound_owners) async fn close_process_lookup(
        &self,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        let original = self.process.lock().unwrap().clone();
        if let Some(original) = original {
            original.cancel();
            original.wait_retired(deadline).await?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "raw-ip-runtime"))]
mod tests;
