//! The candidate retains this original task owner before BuildNew can spawn.
use super::{EndpointStackRecipe, SharedRawIpDevice};
use std::sync::{Arc, Mutex};
use zero_engine::EngineError;
use zero_platform_tokio::contract::lifecycle::CloseReport;

pub(crate) struct EndpointStackOwner {
    workers: Arc<super::super::workers::DeviceWorkers>,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    attempted: bool,
    stopped: bool,
    device: Option<Arc<SharedRawIpDevice>>,
    failure: Option<Arc<EngineError>>,
}
impl EndpointStackOwner {
    /// Pure finite declaration. JoinSet construction requires no runtime and
    /// creates none of the actual driver, TCP control or retransmission tasks.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            workers: super::super::workers::DeviceWorkers::new(),
            state: Mutex::new(State::default()),
        })
    }
    pub(crate) fn start(
        &self,
        recipe: EndpointStackRecipe,
    ) -> Result<Arc<SharedRawIpDevice>, EngineError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.stopped || state.attempted {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "candidate endpoint stack already attempted or retired",
            )));
        }
        state.attempted = true;
        // The mutex is synchronous across the complete actual registration.
        // A racing stop cannot acknowledge an empty owner before a late spawn.
        match recipe.start_with_workers(self.workers.clone()) {
            Ok(device) => {
                state.device = Some(device.clone());
                Ok(device)
            }
            Err(error) => {
                let error = Arc::new(error);
                state.failure = Some(error.clone());
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
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.attempted && !self.is_drained()
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
        self.workers.is_drained()
    }
    pub(crate) fn revoke(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        if let Some(device) = &state.device {
            device.close_now();
        }
        self.workers.abort();
    }
    /// Joins the exact original driver/control/retransmission handles. An
    /// endpoint stack has no native socket; its listener/carrier closes apart.
    /// Cancellation keeps this owner's device, source and JoinSet in place.
    pub(crate) async fn stop(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, EngineError> {
        self.revoke();
        let (device, failure) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (state.device.clone(), state.failure.clone())
        };
        let joining = async {
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
