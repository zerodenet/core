//! One tracked actor owns startup, handoff readiness and direct-child monitoring.
use crate::{transport::plugin::process, validation::PluginConfig};
use std::{
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    process::Child,
    time::{sleep_until, Instant},
};
use zero_platform_tokio::contract::{
    component::ResourceAccess,
    lifecycle::{CloseReport, ResourceLifecycle},
    scope::{TaskJoinWitness, TaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
mod actor;
#[derive(Default)]
struct State {
    attempted: bool,
    child: Option<Child>,
    pid: Option<u32>,
    exited: Option<ExitStatus>,
}
struct Shared {
    state: Mutex<State>,
    cancel: CancellationToken,
}
impl Shared {
    fn status(&self) -> Result<Option<ExitStatus>, ChannelError> {
        let mut state = self.state.lock().unwrap();
        if let Some(status) = state.exited {
            return Ok(Some(status));
        }
        let status = state
            .child
            .as_mut()
            .ok_or_else(|| error(Stage::Receive, Reason::Internal, "plugin child absent"))?
            .try_wait()
            .map_err(|source| io(Stage::Receive, source))?;
        state.exited = status;
        Ok(status)
    }
    fn stop(&self) {
        self.cancel.cancel();
        if let Some(child) = self.state.lock().unwrap().child.as_mut() {
            let _ = child.start_kill();
        }
    }
}
pub(super) struct Owner {
    config: PluginConfig,
    shared: Arc<Shared>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    actor: Option<TaskJoinWitness>,
}
impl Owner {
    pub(super) fn new(
        config: PluginConfig,
        resources: Arc<dyn ResourceAccess>,
        tasks: TaskRegistrar,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            config,
            resources,
            tasks,
            actor: None,
            shared: Arc::new(Shared {
                state: Mutex::new(Default::default()),
                cancel: cancel.child_token(),
            }),
        }
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.stop();
    }
} // request only; no close receipt
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.attempted || self.shared.cancel.is_cancelled() {
                return Err(error(
                    Stage::Prepare,
                    Reason::Revoked,
                    "plugin start is terminal",
                ));
            }
            state.attempted = true;
        }
        self.start_actor(deadline.min(Instant::now() + Duration::from_secs(10)))
            .await
    }

    fn revoke(&self, _: CloseReason) {
        self.shared.stop();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.shared.stop();
        if let Some(actor) = &self.actor {
            let joined = tokio::time::timeout_at(deadline, actor.wait())
                .await
                .map_err(|source| {
                    error(
                        Stage::Close,
                        Reason::Timeout,
                        "plugin actor join remains pending",
                    )
                    .with_source(source)
                })?;
            if !actor.matches_joined(&joined) {
                return Err(error(
                    Stage::Close,
                    Reason::StalePlan,
                    "plugin actor returned a foreign join",
                ));
            }
        }
        if self.shared.state.lock().unwrap().child.is_none() {
            return Ok(Default::default());
        }
        loop {
            if let Some(status) = self.shared.status()? {
                let pid = self.shared.state.lock().unwrap().pid.unwrap();
                return Ok(CloseReport {
                    released_resources: vec![format!(
                        "original SIP003 child {pid} reaped: {status}"
                    )],
                    ..Default::default()
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(error(
                    Stage::Close,
                    Reason::Timeout,
                    "original plugin child remains unconfirmed",
                ));
            }
            sleep_until(deadline.min(now + Duration::from_millis(10))).await;
        }
    }
}
#[derive(Debug)]
struct ChildExited(ExitStatus);
impl std::fmt::Display for ChildExited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original plugin child exited: {}", self.0)
    }
}
impl std::error::Error for ChildExited {}
fn error(stage: Stage, reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component).with_source(std::io::Error::other(message))
}
fn io(stage: Stage, source: std::io::Error) -> ChannelError {
    ChannelError::new(stage, Reason::Io, Impact::Component).with_source(source)
}

struct StartupAttempt(Arc<Shared>, bool);
impl Drop for StartupAttempt {
    fn drop(&mut self) {
        if self.1 {
            self.0.stop();
        }
    }
}

#[cfg(all(test, unix))]
#[path = "../../../../tests/component/inbound_plugin.rs"]
mod tests;
