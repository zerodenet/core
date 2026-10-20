//! The candidate retains this original task owner before BuildNew can spawn.
use super::{EndpointStackRecipe, SharedRawIpDevice};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{lifecycle::CloseReport, scope::WeakTaskRegistrar};

pub(crate) struct EndpointStackOwner {
    workers: Arc<super::super::workers::DeviceWorkers>,
    state: Mutex<State>,
    build_finished: Notify,
}
#[derive(Default)]
struct State {
    attempted: bool,
    building: bool,
    stopped: bool,
    device: Option<Arc<SharedRawIpDevice>>,
    failure: Option<Arc<EngineError>>,
}
impl EndpointStackOwner {
    pub(crate) const TASK_LIMIT: usize = super::super::workers::WORKER_LIMIT;
    /// Pure finite declaration. JoinSet construction requires no runtime and
    /// creates none of the actual driver, TCP control or retransmission tasks.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            workers: super::super::workers::DeviceWorkers::new(),
            state: Mutex::new(State::default()),
            build_finished: Notify::new(),
        })
    }
    /// Cold candidate declaration. Only its actual host may supply admission.
    pub(crate) fn suspended() -> Arc<Self> {
        Arc::new(Self {
            workers: super::super::workers::DeviceWorkers::suspended(),
            state: Mutex::new(State::default()),
            build_finished: Notify::new(),
        })
    }
    pub(crate) fn bind_scope(&self, registrar: WeakTaskRegistrar) -> Result<(), EngineError> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.attempted || state.stopped {
            return Err(retired());
        }
        // Pure weak-authority installation; no Scope callback occurs here.
        self.workers.bind_scope(registrar)
    }
    pub(crate) fn start(
        &self,
        recipe: EndpointStackRecipe,
    ) -> Result<Arc<SharedRawIpDevice>, EngineError> {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.stopped || state.attempted {
                return Err(retired());
            }
            state.attempted = true;
            state.building = true;
        }
        // The same candidate retains all original tasks during this call, but
        // no resource lock spans host registration or its notification callbacks.
        let _attempt = BuildAttempt(self);
        match recipe.start_with_workers(self.workers.clone()) {
            Ok(device) => {
                let stopped = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state.device = Some(device.clone());
                    state.stopped
                };
                if stopped {
                    device.close_now();
                    return Err(retired());
                }
                Ok(device)
            }
            Err(error) => {
                // TcpTaskOwner's compatibility bool may have erased the
                // registration error from the stack constructor. Keep the
                // actual Scope source, already saved by this exact owner.
                let error = Arc::new(self.workers.first_scoped_failure().unwrap_or(error));
                self.state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .failure = Some(error.clone());
                self.workers.abort();
                Err(EngineError::Io(std::io::Error::other(SharedFailure(error))))
            }
        }
    }
    pub(crate) fn is_open(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stopped
    }
    pub(crate) fn needs_cleanup(&self) -> bool {
        let attempted = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .attempted;
        attempted && !self.is_drained()
    }
    pub(crate) fn device(&self) -> Option<Arc<SharedRawIpDevice>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .device
            .clone()
    }
    pub(crate) fn owns(&self, device: &Arc<SharedRawIpDevice>) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .device
            .as_ref()
            .is_some_and(|owned| Arc::ptr_eq(owned, device))
    }
    /// This exact owner consumed all original handles; it may still retain an
    /// original task failure. It says nothing about listener/carrier closure.
    pub(crate) fn is_drained(&self) -> bool {
        let building = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .building;
        !building && self.workers.is_drained()
    }
    pub(crate) fn revoke(&self) {
        let device = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.stopped = true;
            state.device.clone()
        };
        if let Some(device) = device {
            device.close_now();
        }
        self.workers.abort();
    }
    async fn wait_build(&self) {
        loop {
            let changed = self.build_finished.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .building
            {
                return;
            }
            changed.await;
        }
    }
    /// Joins the exact original driver/control/retransmission handles. An
    /// endpoint stack has no native socket; its listener/carrier closes apart.
    /// Cancellation keeps this owner's device, source and JoinSet in place.
    pub(crate) async fn stop(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, EngineError> {
        self.revoke();
        let joining = async {
            // A late registration admitted before revoke remains part of the
            // original attempt. Never acknowledge an empty owner ahead of it.
            self.wait_build().await;
            let (device, failure) = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                (state.device.clone(), state.failure.clone())
            };
            if let Some(device) = device {
                device.tcp.shutdown().await;
            }
            let joined = self.workers.join().await;
            match (failure, joined) {
                (None, result) => result,
                (Some(first), Ok(_)) => {
                    Err(EngineError::Io(std::io::Error::other(SharedFailure(first))))
                }
                (Some(first), Err(join)) => Err(EngineError::Io(std::io::Error::other(
                    BuildAndJoinFailure { first, join },
                ))),
            }
        };
        tokio::time::timeout_at(deadline, joining)
            .await
            .map_err(|source| {
                EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source))
            })?
    }
}
struct BuildAttempt<'a>(&'a EndpointStackOwner);
impl Drop for BuildAttempt<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .building = false;
        self.0.build_finished.notify_waiters();
    }
}
fn retired() -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "candidate endpoint stack already attempted or retired",
    ))
}
#[derive(Debug)]
struct SharedFailure(Arc<EngineError>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[cfg(test)]
mod tests;

#[derive(Debug)]
struct BuildAndJoinFailure {
    first: Arc<EngineError>,
    join: EngineError,
}
impl std::fmt::Display for BuildAndJoinFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "endpoint construction: {}; actual worker join: {}",
            self.first, self.join
        )
    }
}
impl std::error::Error for BuildAndJoinFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.first.as_ref())
    }
}
