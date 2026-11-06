//! Same actual Process entry; no secondary registry or process/network owner.
use super::*;
mod retirement;
use retirement::Retirement;
use zero_platform_tokio::owned_process::{ProcessEndpointHandoff, ProcessEndpointSpec};
pub(super) struct Endpoint {
    pub(super) ordinal: usize,
    pub(super) spec: ProcessEndpointSpec,
    pub(super) original: Option<Arc<ProcessEndpointHandoff>>,
    pub(super) reported: bool,
    retirement: Option<Arc<Retirement>>,
}
pub(super) fn prepare(
    declarations: &[ResourceDeclaration],
    entries: &mut BTreeMap<PathBuf, Entry>,
    identity: Option<&(ComponentInstanceId, TaskScopeIdentity)>,
) -> Result<(), ChannelError> {
    for (ordinal, declaration) in declarations.iter().enumerate() {
        let ResourceDeclaration::ProcessEndpoint {
            process_ordinal,
            spec,
        } = declaration
        else {
            continue;
        };
        let failure = || {
            ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(io::Error::other(
                "endpoint requires its one actual declared Process",
            ))
        };
        let Some(ResourceDeclaration::Process {
            program,
            max_starts,
        }) = declarations.get(*process_ordinal)
        else {
            return Err(failure());
        };
        let entry = entries.get_mut(program).ok_or_else(failure)?;
        if entry.ordinal != *process_ordinal
            || max_starts.get() != 1
            || entry.endpoint.is_some()
            || (!spec.stream && !spec.datagram)
        {
            return Err(failure());
        }
        entry.endpoint = Some(Endpoint {
            ordinal,
            spec: spec.clone(),
            original: None,
            reported: false,
            retirement: identity.map(|(instance, scope)| {
                Retirement::new(*instance, *process_ordinal, scope.clone())
            }),
        });
    }
    Ok(())
}
impl HostResources {
    pub(in crate::component) fn reserve_endpoint(
        &self,
        ordinal: usize,
        tasks: &TaskRegistrar,
        actor: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<Arc<ProcessEndpointHandoff>, ChannelError> {
        if !self.processes.validate(tasks)
            || !tasks.scope_identity().same_scope(&actor.scope_identity())
            || actor.is_joined()
        {
            return Err(self.denied());
        }
        if let Some(paid) = self.processes.prepaid.get() {
            paid.bind_actor(actor)
                .map_err(|source| self.denied().with_source(source))?;
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed
            || self.cancel.is_cancelled()
            || state.start_deadline.is_none_or(|end| Instant::now() >= end)
        {
            return Err(self.denied());
        }
        let entries = self.processes.lock();
        let (program, entry) = entries
            .iter()
            .find(|(_, entry)| {
                entry
                    .endpoint
                    .as_ref()
                    .is_some_and(|e| e.ordinal == ordinal)
            })
            .ok_or_else(|| self.denied())?;
        if !entry.jobs.is_empty()
            || entry
                .endpoint
                .as_ref()
                .is_some_and(|e| e.original.is_some())
        {
            return Err(self.denied());
        }
        let program = program.clone();
        let process_ordinal = entry.ordinal;
        let spec = entry
            .endpoint
            .as_ref()
            .expect("same endpoint ordinal")
            .spec
            .clone();
        let instance = self
            .processes
            .identity
            .as_ref()
            .expect("validated actual Process instance")
            .0;
        drop(entries);
        drop(state);
        // Scope/provider getters and cold owner construction stay outside every
        // Host/entry lock. The winner is installed atomically before native IO.
        let original = ProcessEndpointHandoff::new(
            instance,
            process_ordinal,
            spec,
            tasks,
            self.cancel.clone(),
        )?;
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed
            || self.cancel.is_cancelled()
            || state.start_deadline.is_none_or(|end| Instant::now() >= end)
        {
            return Err(self.denied());
        }
        let mut entries = self.processes.lock();
        let entry = entries.get_mut(&program).expect("same declared Process");
        if !entry.jobs.is_empty()
            || entry
                .endpoint
                .as_ref()
                .is_some_and(|e| e.original.is_some())
        {
            return Err(self.denied());
        }
        entry
            .endpoint
            .as_mut()
            .expect("same endpoint ordinal")
            .original = Some(original.clone());
        drop(entries);
        drop(state);
        // Charge the same actual Process original BEFORE any endpoint socket.
        // The placeholder cannot execute: endpoint starts require one real
        // request installation, explicit reservation receipts and same actor.
        let process = self.prepare_process_original(ProcessRequest::new(program), tasks)?;
        if let Err(source) = process.bind_actor(actor.clone()) {
            process.request_stop();
            original.request_stop();
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "actual endpoint actor binding failed",
            )
            .with_source(source));
        }
        if let Err(source) = process.attach_endpoint(&original) {
            process.request_stop();
            original.request_stop();
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "same endpoint job attachment failed",
            )
            .with_source(source));
        }
        original.acquire(tasks, actor)?;
        Ok(original)
    }
    pub(in crate::component) fn prepare_process_with_endpoint(
        &self,
        request: ProcessRequest,
        tasks: &TaskRegistrar,
        endpoint: &Arc<ProcessEndpointHandoff>,
    ) -> Result<Arc<OwnedProcess>, ChannelError> {
        if !self.processes.validate(tasks) {
            return Err(self.denied());
        }
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed
                || self.cancel.is_cancelled()
                || state.start_deadline.is_none_or(|end| Instant::now() >= end)
            {
                return Err(self.denied());
            }
        }
        let matches = {
            let entries = self.processes.lock();
            entries.get(&request.program).is_some_and(|entry| {
                entry.ordinal == endpoint.process_ordinal()
                    && entry
                        .endpoint
                        .as_ref()
                        .and_then(|e| e.original.as_ref())
                        .is_some_and(|actual| Arc::ptr_eq(actual, endpoint))
            })
        };
        if !matches {
            return Err(self.denied());
        }
        let original = {
            let entries = self.processes.lock();
            entries
                .get(&request.program)
                .and_then(|entry| entry.jobs.first())
                .map(|job| job.original.clone())
                .ok_or_else(|| self.denied())?
        };
        if let Err(source) = original.install_endpoint_request(request, endpoint) {
            original.request_stop();
            endpoint.request_stop();
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "actual Process endpoint request installation failed",
            )
            .with_source(source));
        }
        Ok(original)
    }
    pub(in crate::component) fn process_endpoint(
        &self,
        process_ordinal: usize,
    ) -> Option<Arc<ProcessEndpointHandoff>> {
        self.processes
            .lock()
            .values()
            .find(|entry| entry.ordinal == process_ordinal)?
            .endpoint
            .as_ref()?
            .original
            .clone()
    }
}
impl ProcessDeclarations {
    pub(super) fn endpoint_evidence(&self) -> Vec<ProcessIoEvidence> {
        self.lock()
            .values()
            .filter_map(|entry| {
                let endpoint = entry.endpoint.as_ref()?;
                Some(ProcessIoEvidence {
                    ordinal: endpoint.ordinal,
                    registered: self.identity.is_some() && !entry.legacy,
                    confirmed: (!entry.legacy)
                        && endpoint.original.as_ref().is_none_or(|original| {
                            original
                                .closed()
                                .is_some_and(|closed| closed.matches(original))
                        }),
                    first_fault: endpoint
                        .original
                        .as_ref()
                        .and_then(|original| original.first_failure()),
                })
            })
            .collect()
    }
    pub(super) fn report_endpoints(&self, component: ComponentId, report: &mut CloseReport) {
        for entry in self.lock().values_mut() {
            let Some(endpoint) = entry.endpoint.as_mut() else {
                continue;
            };
            let Some(original) = endpoint.original.as_ref() else {
                continue;
            };
            if original
                .closed()
                .is_some_and(|closed| closed.matches(original))
            {
                if !endpoint.reported {
                    report.closed_io.push(format!("component {} process endpoint {} actual reservations/probe/derived originals closed",component.0,endpoint.ordinal));
                    endpoint.reported = true;
                }
            } else {
                report.unfinished.push(Unfinished::Resource(format!(
                    "component {} process endpoint {} retains original obligations",
                    component.0, endpoint.ordinal
                )));
            }
        }
    }
}

#[cfg(test)]
#[path = "../../../../tests/native_process/endpoint.rs"]
mod tests;
