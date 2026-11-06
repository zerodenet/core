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
    identity: Option<(ComponentInstanceId, TaskScopeIdentity)>,
    entries: Mutex<BTreeMap<PathBuf, Entry>>,
}
struct Entry {
    ordinal: usize,
    legacy: bool,
    jobs: Vec<Job>,
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
    ) -> Self {
        let entries = declarations
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
                    },
                ))
            })
            .collect();
        Self {
            identity,
            entries: Mutex::new(entries),
        }
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
    pub(super) fn evidence(&self, component: ComponentId) -> Vec<ProcessIoEvidence> {
        self.lock()
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
            .collect()
    }
    pub(super) async fn shutdown(&self, component: ComponentId, deadline: Instant) -> CloseReport {
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
        futures_util::future::join_all(jobs.iter().map(|job| job.wait_closed(deadline))).await;
        let mut report = CloseReport::default();
        for entry in self.lock().values_mut() {
            for job in &mut entry.jobs {
                if job
                    .original
                    .closed()
                    .is_some_and(|closed| closed.matches(&job.original))
                {
                    if !job.reported {
                        report.closed_io.push(format!("component {} process declaration {} actual tree and original descriptors closed", component.0, entry.ordinal));
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
        report
    }
}
impl HostResources {
    pub(super) fn prepare_process(
        &self,
        request: ProcessRequest,
        tasks: &TaskRegistrar,
    ) -> Result<Arc<OwnedProcess>, ChannelError> {
        // Original Scope observations occur outside host locks: upgrading a
        // weak scope can deliver callbacks when its last owner is released.
        if !self.processes.validate(tasks) {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "native process owner revoked",
            ));
        }
        let deadline = state.start_deadline.ok_or_else(|| {
            error(
                self.component,
                Reason::InvalidConfig,
                "owned process requires actual start phase",
            )
        })?;
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "owned process start deadline elapsed",
            ));
        }
        let remaining = state
            .remaining
            .get_mut(&request.program)
            .ok_or_else(|| self.denied())?;
        if *remaining == 0 {
            return Err(error(
                self.component,
                Reason::Capacity,
                "declared process original budget exhausted",
            ));
        }
        let program = request.program.clone();
        let mut entries = self.processes.lock();
        let entry = entries.get_mut(&program).ok_or_else(|| self.denied())?;
        if entry.legacy {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "legacy process has no tree original",
            ));
        }
        let original = OwnedProcess::prepare(request, tasks).map_err(|source| {
            error(
                self.component,
                Reason::Io,
                "cold process original preparation failed",
            )
            .with_source(source)
        })?;
        entry.jobs.push(Job {
            original: original.clone(),
            reported: false,
        });
        *remaining -= 1; // pending/failed/retiring reservations are not refunded
        Ok(original)
    }
}
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
