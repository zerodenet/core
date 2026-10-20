//! Every branch persists receipts before returning to the cancelling caller.
use super::*;
use futures_util::future::join_all;
use tokio::time::timeout_at;
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};

#[derive(Default)]
pub(super) struct NativeClose {
    pub(super) confirmed: std::collections::BTreeSet<(bool, SocketAddr)>,
    pub(super) receipts: CloseReport,
    // At most one retained first failure per declared native resource. The
    // actual owner separately retains the current OS source and original FD.
    pub(super) failures: BTreeMap<(bool, SocketAddr), Arc<ChannelError>>,
}
impl HostResources {
    pub(in crate::component) async fn shutdown_io(
        &self,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.start_deadline = None;
        }
        // Fence the actual registered physical owner as well as new access.
        // Cancellation callbacks run after the durable closed phase is installed
        // and outside every host resource/cleanup lock.
        self.carriers.revoke();
        self.changed.notify_waiters();
        // Native observations may flush external wakes. Sample before acquiring
        // even the async cleanup gate; a concurrently completed receipt can be
        // observed on retry, never guessed from an older pending snapshot.
        let carriers = self.carriers.evidence();
        self.metadata.close_native(self.component);
        let metadata = self.metadata.evidence(self.component);
        let Ok(_cleanup) = timeout_at(deadline, self.cleanup.lock()).await else {
            return Ok(unfinished("native/file cleanup owner is still busy".into()));
        };
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .io
            .receipts
            .unfinished
            .clear();
        // Neither an IO failure nor a slow file job prevents driving the other
        // actual owners. No tasks are spawned; these futures stay structured.
        tokio::join!(self.drive_native(deadline), self.drive_files(deadline));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Preserve the existing exact JoinError chain. Its real join receipt
        // stays in FileReads on Err, alongside every native receipt and error.
        if let Some(source) = &state.files.failure {
            return Err(crate::component::instance::retained(source.clone()));
        }
        if let Some(source) = state.io.failures.values().next() {
            return Err(crate::component::instance::retained(source.clone()));
        }
        if let Some(source) = carriers
            .iter()
            .find_map(|carrier| carrier.first_fault.as_ref())
        {
            return Err(crate::component::instance::retained(source.clone()));
        }
        if let Some(source) = metadata.iter().find_map(|entry| entry.first_fault.as_ref()) {
            return Err(crate::component::instance::retained(source.clone()));
        }
        let mut report = std::mem::take(&mut state.io.receipts);
        report.append(std::mem::take(&mut state.files.receipts));
        report.unfinished.extend(
            state
                .files
                .evidence()
                .into_iter()
                .filter(|file| !file.physical_confirmed())
                .map(|file| {
                    Unfinished::Resource(format!(
                        "component {} file {} job/native cleanup remains unconfirmed",
                        self.component.0,
                        file.path.display()
                    ))
                }),
        );
        report.unfinished.extend(carriers.iter().filter(|carrier| !carrier.physical_confirmed()).map(|carrier| Unfinished::Resource(format!(
            "component {} carrier declaration {} native_closed={} transferred users remain unaccounted",
            self.component.0, carrier.ordinal, carrier.native_generation_closed
        ))));
        drop(state);
        self.metadata.append_receipts(self.component, &mut report);
        report.unfinished.extend(metadata.iter().filter(|entry| !entry.physical_confirmed()).map(|entry| Unfinished::Resource(format!("component {} metadata declaration {} native_busy={} unconfirmed users={:?} confirmed private user graphs={}",self.component.0,entry.ordinal,entry.busy,entry.unconfirmed_users(),entry.confirmed_users()))));
        Ok(report)
    }
    async fn drive_native(&self, deadline: Instant) {
        let (tcp, udp) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state
                    .tcp
                    .iter()
                    .map(|(address, owner)| (*address, owner.clone()))
                    .collect::<Vec<_>>(),
                state
                    .datagram
                    .iter()
                    .map(|(address, owner)| (*address, owner.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        let mut pending: Vec<BoxFuture<'_, ()>> = Vec::with_capacity(tcp.len() + udp.len());
        for (address, owner) in tcp {
            pending.push(Box::pin(async move {
                let result = match timeout_at(deadline, owner.lock()).await {
                    Ok(mut owner) => owner.native.wait_closed(deadline).await,
                    Err(_) => Ok(unfinished(format!(
                        "TCP listener {address} owner is still busy"
                    ))),
                };
                self.record_native((true, address), result);
            }));
        }
        for (address, owner) in udp {
            pending.push(Box::pin(async move {
                let result = match timeout_at(deadline, owner.lock()).await {
                    Ok(mut owner) => owner.shutdown(self.component, address, deadline),
                    Err(_) => Ok(unfinished(format!(
                        "UDP listener {address} owner is still busy"
                    ))),
                };
                self.record_native((false, address), result);
            }));
        }
        join_all(pending).await;
    }
    fn record_native(&self, key: (bool, SocketAddr), result: Result<CloseReport, ChannelError>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match result {
            Ok(report) => {
                if report.is_complete() {
                    state.io.confirmed.insert(key);
                }
                state.io.receipts.append(report);
            }
            Err(mut failure) => {
                state.io.confirmed.remove(&key);
                failure.component.get_or_insert(self.component);
                state
                    .io
                    .failures
                    .entry(key)
                    .or_insert_with(|| Arc::new(failure));
            }
        }
    }
}
fn unfinished(label: String) -> CloseReport {
    CloseReport {
        unfinished: vec![Unfinished::Resource(label)],
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "../../../tests/resource_files/sticky.rs"]
mod sticky_tests;
