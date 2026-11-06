//! Persist the exact original registration before opening its task start gate.
use super::*;
impl OwnedDnsQueryTasks {
    pub(super) fn spawn_kind(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: Option<u64>,
        kind: TaskKind,
    ) -> io::Result<TaskJoinWitness> {
        self.ensure_open()?;
        #[cfg(feature = "doh")]
        let ancestors = if kind == TaskKind::Driver {
            Vec::new()
        } else {
            self.inherited_tasks()?
        };
        #[cfg(not(feature = "doh"))]
        let ancestors = self.inherited_tasks()?;
        let record = Arc::new(Record {
            budget: self.0.budget.clone(),
            // Only persistent physical drivers inherit an explicit endpoint;
            // nested bootstrap queries retain their own configured targets.
            ech: Mutex::new(if matches!(kind, TaskKind::Query | TaskKind::Blocking) {
                None
            } else {
                QUERY_RECORD
                    .try_with(|record| record.ech.lock().expect("DNS ECH target poisoned").clone())
                    .ok()
                    .flatten()
            }),
            capacity: Mutex::new(Some(self.0.budget.reserve(kind)?)),
            kind,
            ancestors,
            network: network.map(OnceLock::from).unwrap_or_default(),
            registration: OnceLock::new(),
            bound: Notify::new(),
            cancel,
            io_pending: Default::default(),
            original: Mutex::new(None),
            metadata: Mutex::new(None),
        });
        self.0
            .budget
            .retained
            .lock()
            .expect("DNS budget poisoned")
            .push(Arc::downgrade(&record));
        {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.closing || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS query admission retired",
                ));
            }
            if state
                .records
                .iter()
                .filter(|record| record.kind == kind)
                .count()
                >= self.0.limit.get()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "DNS query task capacity exhausted",
                ));
            }
            state.records.try_reserve(1).map_err(io::Error::other)?;
            // Install before the original actual spawn; an unjoined record is
            // never discarded by cancellation or a shutdown waiter timeout.
            state.records.push(record.clone());
        }
        let (sender, receiver) = oneshot::channel::<TaskJoinWitness>();
        let parent = self.0.parent.clone();
        let cancellation = record.cancel.clone();
        let query_record = record.clone();
        let callbacks = self.0.registrar.defer_callbacks();
        let finalizer = self.0.finalizer.get().cloned();
        let spawned = self.0.registrar.spawn_tracked(self.0.class, async move {
            let Ok(witness) = receiver.await else {
                return Ok(());
            };
            let caller = witness.clone();
            // No original resolution/native I/O can run before the same spawn's
            // witness has been persisted and delivered by its original caller.
            QUERY_RECORD
                .scope(
                    query_record,
                    QUERY_CANCEL.scope(
                        cancellation.clone(),
                        QUERY_ORIGIN.scope(witness, async move {
                            tokio::select! { biased;
                                _ = parent.cancelled() => {},
                                _ = cancellation.cancelled() => {},
                                _ = job => {},
                            }
                        }),
                    ),
                )
                .await;
            // This stays in the original registered task, outside the cancelled
            // work future. Actual caller join is published only after cleanup.
            if let Some(finalizer) = finalizer {
                let provider = finalizer.upgrade().ok_or_else(|| {
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Revoked,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                })?;
                provider.finish_query(caller).await.map_err(|source| {
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Io,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
            }
            Ok(())
        });
        match spawned {
            Ok((id, witness)) => {
                record
                    .registration
                    .set(Some(QueryRegistration {
                        id,
                        witness: witness.clone(),
                    }))
                    .expect("query bound once");
                record.bound.notify_waiters();
                let _ = sender.send(witness.clone());
                drop(callbacks);
                Ok(witness)
            }
            Err(source) => {
                // Failed synchronous registration never started a task/native.
                record
                    .registration
                    .set(None)
                    .expect("failed query registration settled once");
                record.bound.notify_waiters();
                let retired = {
                    let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                    let index = state
                        .records
                        .iter()
                        .position(|current| Arc::ptr_eq(current, &record));
                    index.map(|index| state.records.remove(index))
                };
                record.cancel.cancel();
                drop(retired);
                drop(callbacks);
                Err(io::Error::other(source))
            }
        }
    }
}
