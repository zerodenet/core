//! Actual process originals in this generation's existing declaration budget.
use super::*;
use std::io;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, Unfinished},
        scope::{TaskRegistrar, TaskScopeIdentity},
    },
    owned_process::OwnedProcess,
};

pub(super) struct ProcessDeclarations {
    budget: Arc<zero_platform_tokio::owned_process::ProcessBudget>,
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    entries: Mutex<BTreeMap<PathBuf, Entry>>,
}
struct Entry {
    ordinal: usize,
    legacy: bool,
    jobs: Vec<Job>,
    endpoint: Option<endpoint::Endpoint>,
}
struct Job {
    original: Arc<OwnedProcess>,
    reported: bool,
}
#[derive(Clone, Debug)]
pub(in crate::component) struct ProcessIoEvidence {
    pub ordinal: usize,
    pub registered: bool,
    pub confirmed: bool,
    pub first_fault: Option<Arc<ChannelError>>,
}
impl ProcessDeclarations {
    pub(super) fn new(
        declarations: &[ResourceDeclaration],
        identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
        budget: Arc<zero_platform_tokio::owned_process::ProcessBudget>,
    ) -> Result<Self, ChannelError> {
        let mut entries: BTreeMap<PathBuf, Entry> = declarations
            .iter()
            .enumerate()
            .filter_map(|(ordinal, declaration)| {
                let ResourceDeclaration::Process { program, .. } = declaration else {
                    return None;
                };
                Some((
                    program.clone(),
                    Entry {
                        ordinal,
                        legacy: false,
                        jobs: Vec::new(),
                        endpoint: None,
                    },
                ))
            })
            .collect();
        endpoint::prepare(declarations, &mut entries)?;
        Ok(Self {
            budget,
            identity,
            entries: Mutex::new(entries),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<PathBuf, Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn mark_legacy(&self, program: &PathBuf) {
        if let Some(entry) = self.lock().get_mut(program) {
            entry.legacy = true;
        }
    }
    fn validate(&self, tasks: &TaskRegistrar) -> bool {
        self.identity
            .as_ref()
            .is_some_and(|(_, scope)| scope.matches(tasks) && scope.resource_open())
    }
    pub(super) fn poll_budget(&self, cx: &mut std::task::Context<'_>) {
        self.budget.poll_released(cx);
    }
    pub(super) fn evidence(&self, component: ComponentId) -> Vec<ProcessIoEvidence> {
        let mut evidence: Vec<_> = self
            .lock()
            .values()
            .map(|entry| ProcessIoEvidence {
                ordinal: entry.ordinal,
                registered: self.identity.is_some() && !entry.legacy,
                confirmed: self.identity.is_some()
                    && !entry.legacy
                    && entry.jobs.iter().all(|job| {
                        job.original
                            .closed()
                            .is_some_and(|closed| closed.matches(&job.original))
                    }),
                first_fault: entry
                    .jobs
                    .iter()
                    .filter(|job| job.original.closed().is_none())
                    .find_map(|job| job.original.first_failure())
                    .map(|source| {
                        Arc::new(
                            error(
                                component,
                                Reason::Io,
                                "actual process original retained its failure",
                            )
                            .with_source(ProcessFailure(source)),
                        )
                    }),
            })
            .collect();
        evidence.extend(self.endpoint_evidence());
        evidence
    }
    pub(super) async fn shutdown(&self, component: ComponentId, deadline: Instant) -> CloseReport {
        let endpoints: Vec<_> = {
            self.lock()
                .values()
                .filter_map(|entry| entry.endpoint.as_ref()?.original.clone())
                .collect()
        };
        for endpoint in &endpoints {
            endpoint.request_stop();
        }
        let jobs: Vec<_> = self
            .lock()
            .values()
            .flat_map(|entry| entry.jobs.iter().map(|job| job.original.clone()))
            .collect();
        for job in &jobs {
            job.request_stop();
        }
        // Structured progress in the same existing host cleanup, no provider
        // task, actor self-join, separate runtime or replacement pool.
        let (endpoint_results, _) = tokio::join!(
            futures_util::future::join_all(
                endpoints.iter().map(|endpoint| endpoint.shutdown(deadline))
            ),
            futures_util::future::join_all(jobs.iter().map(|job| job.wait_closed(deadline)))
        );
        let mut report = CloseReport::default();
        // Endpoint originals retain every original failure for io_evidence,
        // independently of matching physical completion. No report text is proof.
        drop(endpoint_results);
        for entry in self.lock().values_mut() {
            for job in &mut entry.jobs {
                if let Some(closed) = job
                    .original
                    .closed()
                    .filter(|closed| closed.matches(&job.original))
                {
                    if !job.reported {
                        if closed.had_native_acquisition() {
                            report.closed_io.push(format!("component {} process declaration {} actual tree and original descriptors closed", component.0, entry.ordinal));
                        }
                        job.reported = true;
                    }
                } else {
                    report.unfinished.push(Unfinished::Resource(format!(
                        "component {} process declaration {} retains its actual original",
                        component.0, entry.ordinal
                    )));
                }
            }
        }
        self.report_endpoints(component, &mut report);
        report
    }
}
mod acquire;
mod endpoint;
#[derive(Debug)]
struct ProcessFailure(Arc<io::Error>);
impl std::fmt::Display for ProcessFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ProcessFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[cfg(test)]
#[path = "../../../tests/native_process/owned.rs"]
mod tests;
