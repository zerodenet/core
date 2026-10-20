//! Physical proof contains only identity, counters and original failure evidence.
use super::*;
#[derive(Debug, Clone)]
pub(crate) struct InboundTaskFailure {
    pub(crate) id: TaskId,
    pub(crate) class: TaskClass,
    pub(crate) source: SharedError,
}
#[derive(Debug)]
pub(crate) struct InboundTerminal {
    identity: ComponentInstanceId,
    observation: Arc<observation::ObservationDisposition>,
    joined_tasks: usize,
    closed_io_receipts: usize,
    failure: Option<SharedError>,
    tasks: Box<[InboundTaskFailure]>,
    client_failure: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    native_bound_failures: Box<[SharedError]>,
}
impl InboundTerminal {
    pub(in crate::runtime::inbound_owners) fn publish(&self) {
        self.observation.publish();
    }
    pub(crate) fn identity(&self) -> ComponentInstanceId {
        self.identity
    }
    pub(crate) fn failure(&self) -> Option<SharedError> {
        self.failure.clone()
    }
    pub(crate) fn native_bound_failures(&self) -> &[SharedError] {
        &self.native_bound_failures
    }
    pub(crate) fn client_failure(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        self.client_failure.clone()
    }
    pub(crate) fn task_failures(&self) -> &[InboundTaskFailure] {
        &self.tasks
    }
    pub(crate) fn joined_tasks(&self) -> usize {
        self.joined_tasks
    }
    pub(crate) fn closed_io_receipts(&self) -> usize {
        self.closed_io_receipts
    }
}
/// A caller may own a capability, but only the registry can issue/consume it.
/// It never holds the old owner, configuration, engine or resource handles.
#[derive(Debug)]
pub(crate) struct ClosedInboundReceipt {
    pub(in crate::runtime::inbound_owners) tag: String,
    pub(in crate::runtime::inbound_owners) terminal: Arc<InboundTerminal>,
}
impl ClosedInboundReceipt {
    pub(crate) fn terminal(&self) -> &InboundTerminal {
        &self.terminal
    }
    /// Same immutable original evidence, without the registry release/ACK
    /// capability. Cloning it cannot release an owner or issue another receipt.
    pub(crate) fn terminal_handle(&self) -> Arc<InboundTerminal> {
        self.terminal.clone()
    }
}
impl InboundOwner {
    /// Returns whether this actual exit is a component/listener failure.
    /// Ordinary accepted Auxiliary rejections have an explicit local receiver.
    pub(super) fn record(&self, exit: TaskExit) -> bool {
        // Acknowledge callbacks run before taking host report/resource
        // locks. The same entry persisted its real TaskJoined before delivery.
        let accepted = self
            .accepted_receiver
            .get()
            .and_then(|receiver| receiver.observe_joined(exit.id));
        if let Some(joined) = &accepted {
            if joined.result().is_err() {
                self.client_failure.get_or_init(|| joined.clone());
            }
        }
        if accepted
            .as_ref()
            .is_some_and(|joined| joined.class() == TaskClass::Auxiliary)
        {
            let mut count = self
                .joined_count
                .lock()
                .expect("inbound joined count poisoned");
            *count = count.saturating_add(1);
            drop(count);
            self.ready.wake();
            return false;
        }
        let mut report = self.report.lock().expect("inbound close report poisoned");
        let mut joined = self
            .joined_count
            .lock()
            .expect("inbound joined count poisoned");
        *joined = joined.saturating_add(1);
        drop(joined);
        let failed = exit.result.is_err();
        let failure = if let Err(mut error) = exit.result {
            error.component.get_or_insert(self.identity.component);
            let error = Arc::new(error);
            self.task_failures
                .lock()
                .expect("inbound task faults poisoned")
                .insert(
                    exit.id,
                    InboundTaskFailure {
                        id: exit.id,
                        class: exit.class,
                        source: error.clone(),
                    },
                );
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert(error.clone());
            report.failed_tasks.push(TaskFailure {
                id: exit.id,
                class: exit.class,
                error: retained(error.clone()),
            });
            Some(error)
        } else {
            None
        };
        #[cfg(feature = "managed-datagram-runtime")]
        let peer = self
            .peers
            .lock()
            .expect("peer owners poisoned")
            .iter()
            .any(|group| group.joined(exit.id, exit.class, failure.clone()));
        #[cfg(not(feature = "managed-datagram-runtime"))]
        let peer = {
            let _ = failure;
            false
        };
        // A successful dynamic peer's actual exit is consumed by its group;
        // keep only bounded active IDs and sticky failed receipts, not history.
        if !peer || failed {
            report.exited_tasks.push(exit.id);
        }
        drop(report);
        self.ready.wake();
        if self.driver.get() == Some(&exit.id) {
            self.driver_finished.store(true, Ordering::Release);
            self.ready.wake();
        }
        failed
    }
    pub(crate) fn identity(&self) -> ComponentInstanceId {
        self.identity
    }
    pub(super) fn retain_resource_failures(&self, report: &mut CloseReport) {
        for task in std::mem::take(&mut report.failed_tasks) {
            let source = Arc::new(task.error);
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert(source.clone());
            self.task_failures
                .lock()
                .expect("inbound task faults poisoned")
                .entry(task.id)
                .or_insert(InboundTaskFailure {
                    id: task.id,
                    class: task.class,
                    source: source.clone(),
                });
            report.failed_tasks.push(TaskFailure {
                id: task.id,
                class: task.class,
                error: retained(source),
            });
        }
    }
    pub(super) fn confirm_physical_close(&self) -> bool {
        if self.physically_closed.load(Ordering::Acquire) {
            return true;
        }
        let pending = self
            .pending_native_bound
            .lock()
            .expect("pending native listener poisoned");
        if let Some(pending) = pending.as_ref() {
            if !pending.is_closed() {
                return false;
            }
            // A second shutdown caller can see the native close before the
            // original waiter resumes. Transfer its durable evidence under
            // the same report lock before sealing a terminal receipt.
            self.report
                .lock()
                .expect("inbound close report poisoned")
                .append(pending.take_report());
            self.retain_pending_native_faults(pending);
        }
        if !self.installation_complete.load(Ordering::Acquire)
            || !self.cancel.is_cancelled()
            || !self.scope_closed.load(Ordering::Acquire)
        {
            return false;
        }
        drop(pending);
        if self
            .resources
            .lock()
            .expect("inbound resources poisoned")
            .iter()
            .any(|resource| !resource.stopped.load(Ordering::Acquire))
        {
            return false;
        }
        #[cfg(feature = "managed-datagram-runtime")]
        if self
            .peers
            .lock()
            .expect("peer owners poisoned")
            .iter()
            .any(|group| !group.physically_closed())
        {
            return false;
        }
        self.physically_closed.store(true, Ordering::Release);
        true
    }
    #[cfg(test)]
    pub(crate) fn terminal_for_test(&self) -> Option<Arc<InboundTerminal>> {
        self.terminal()
    }

    pub(in crate::runtime::inbound_owners) fn terminal(&self) -> Option<Arc<InboundTerminal>> {
        if !self.physically_closed.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.terminal
                .get_or_init(|| {
                    let closed_io_receipts = self
                        .report
                        .lock()
                        .expect("inbound close report poisoned")
                        .closed_io
                        .len();
                    #[cfg(feature = "managed-datagram-runtime")]
                    let closed_io_receipts = self
                        .peers
                        .lock()
                        .expect("peer owners poisoned")
                        .iter()
                        .fold(closed_io_receipts, |total, group| {
                            total.saturating_add(group.closed_io_receipts_count())
                        });
                    let mut native_bound_failures = self
                        .pending_native_bound
                        .lock()
                        .expect("pending native listener poisoned")
                        .as_ref()
                        .map_or_else(
                            || Vec::new().into_boxed_slice(),
                            |pending| pending.failures(),
                        )
                        .into_vec();
                    native_bound_failures.extend(
                        self.resources
                            .lock()
                            .expect("inbound resources poisoned")
                            .iter()
                            .filter_map(|resource| resource.exit_fault.get().cloned()),
                    );
                    Arc::new(InboundTerminal {
                        native_bound_failures: native_bound_failures.into_boxed_slice(),
                        client_failure: self.client_failure.get().cloned(),
                        identity: self.identity,
                        observation: self.observation.clone(),
                        joined_tasks: *self
                            .joined_count
                            .lock()
                            .expect("inbound joined count poisoned"),
                        closed_io_receipts,
                        failure: self
                            .failure
                            .lock()
                            .expect("inbound failure poisoned")
                            .clone(),
                        tasks: self
                            .task_failures
                            .lock()
                            .expect("inbound task faults poisoned")
                            .values()
                            .cloned()
                            .collect(),
                    })
                })
                .clone(),
        )
    }
}
