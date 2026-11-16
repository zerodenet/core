//! Source protocol and native ancillary cleanup make progress together.
use super::resources::StartAccess;
use super::*;
use std::collections::BTreeMap;
#[derive(Default)]
pub(super) struct Collected {
    closed: CloseReport,
    pending: BTreeMap<&'static str, Vec<zero_platform_tokio::contract::lifecycle::Unfinished>>,
    failures: BTreeMap<&'static str, Arc<ChannelError>>,
    current: BTreeMap<&'static str, Arc<ChannelError>>,
}
async fn retain(
    collected: &Mutex<Collected>,
    stage: &'static str,
    work: impl std::future::Future<Output = Result<CloseReport, BuildError>>,
) {
    collected.lock().unwrap().pending.insert(
        stage,
        vec![
            zero_platform_tokio::contract::lifecycle::Unfinished::Resource(format!(
                "original {stage} cleanup is pending"
            )),
        ],
    );
    let result = work.await;
    collected.lock().unwrap().record(stage, result);
}
impl Collected {
    fn record(&mut self, stage: &'static str, result: Result<CloseReport, BuildError>) {
        match result {
            Ok(mut report) => {
                self.current.remove(stage);
                self.pending
                    .insert(stage, std::mem::take(&mut report.unfinished));
                report
                    .closed_io
                    .retain(|value| !self.closed.closed_io.contains(value));
                report
                    .released_resources
                    .retain(|value| !self.closed.released_resources.contains(value));
                self.closed.append(report);
                // This actual persistent Source row now owns those exact Arcs.
                // Ownership delivery is not publication or physical closure.
                for failure in &self.closed.resource_failures {
                    failure.acknowledge_retained(failure);
                }
            }
            Err(source) => {
                let original = source
                    .retained_source()
                    .cloned()
                    .unwrap_or_else(|| Arc::new(source));
                self.failures
                    .entry(stage)
                    .or_insert_with(|| original.clone());
                self.current.insert(stage, original);
            }
        }
    }
    fn report(&mut self) -> Result<CloseReport, BuildError> {
        if let Some(source) = self.current.values().next() {
            return Err(ChannelError::retained(source.clone()));
        }
        let mut report = std::mem::take(&mut self.closed);
        report.unfinished = self.pending.values().flatten().cloned().collect();
        Ok(report)
    }
}
impl ResourceLifecycle for Lifecycle {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        self.resources.starting.store(true, Ordering::Release);
        let _access = StartAccess(&self.resources.starting);
        let _metadata_access = self
            .resources
            .metadata
            .as_ref()
            .map(|r| r.activate(deadline))
            .transpose()?;
        let _process_access = self
            .resources
            .process
            .as_ref()
            .map(|r| r.activate(deadline))
            .transpose()?;
        if let Some(handoff) = &self.resources.handoff {
            handoff.release_public(deadline).await?;
        }
        self.resources.reads.activate();
        if let Some(files) = &self.resources.files {
            files.start(deadline).await?;
        }
        self.protocol.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.resources.reads.revoke();
        if let Some(targets) = &self.resources.component_targets {
            targets.revoke();
        }
        self.protocol.revoke(reason);
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(udp) = &self.resources.udp {
            udp.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.resources.starting.store(false, Ordering::Release);
        let view = self
            .resources
            .listener
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(view);
        self.resources.extra_tcp.lock().unwrap().clear();
        drop(self.resources.primary_udp.lock().unwrap().take());
        // Each completion is recorded in this persistent owner before another
        // await, including when a sibling errors or shutdown is interrupted.
        let collected = &self.collected;
        tokio::join!(
            retain(collected, "protocol", self.protocol.shutdown(deadline)),
            retain(collected, "files", self.resources.reads.shutdown(deadline)),
            async {
                if let Some(targets) = &self.resources.component_targets {
                    retain(collected, "component targets", targets.shutdown(deadline)).await;
                }
            },
            async {
                if let Some(files) = &self.resources.files {
                    retain(collected, "file tree", files.shutdown(deadline)).await;
                }
            },
        );
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(udp) = &self.resources.udp {
            collected
                .lock()
                .unwrap()
                .record("datagram", udp.close(deadline));
        }
        self.collected.lock().unwrap().report()
    }
}
