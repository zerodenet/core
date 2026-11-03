//! The real Child is installed synchronously before the first readiness await.
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
    scope::{TaskClass, TaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
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
            shared: Arc::new(Shared {
                state: Mutex::new(Default::default()),
                cancel: cancel.child_token(),
            }),
        }
    }
    async fn ready(&self, deadline: Instant) -> Result<(), ChannelError> {
        if !self.config.mode.tcp() {
            if let Some(status) = self.shared.status()? {
                return Err(
                    error(Stage::Prepare, Reason::Io, "plugin exited during readiness")
                        .with_source(ChildExited(status)),
                );
            }
            return Ok(());
        }
        loop {
            if let Some(status) = self.shared.status()? {
                return Err(
                    error(Stage::Prepare, Reason::Io, "plugin exited during readiness")
                        .with_source(ChildExited(status)),
                );
            }
            let probe = self.resources.probe_inbound_listener(deadline);
            tokio::pin!(probe);
            let connected = loop {
                tokio::select! {biased;
                    _=self.shared.cancel.cancelled()=>return Err(error(Stage::Prepare,Reason::Cancelled,"plugin start cancelled")),
                    result=&mut probe=>break result?,
                    _=tokio::time::sleep(Duration::from_millis(10))=>{if let Some(status)=self.shared.status()?{return Err(error(Stage::Prepare,Reason::Io,"plugin exited during pending probe").with_source(ChildExited(status)));}},
                }
            };
            if connected {
                if let Some(status) = self.shared.status()? {
                    return Err(
                        error(Stage::Prepare, Reason::Io, "plugin exited after probe")
                            .with_source(ChildExited(status)),
                    );
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(error(
                    Stage::Prepare,
                    Reason::Timeout,
                    "plugin readiness timed out",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
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
        let endpoints = self.resources.inbound_endpoints()?;
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
            let local = if self.config.mode.tcp() {
                endpoints.stream
            } else {
                endpoints.datagram
            };
            let request = process::request(
                &self.config,
                (
                    &endpoints.external.ip().to_string(),
                    endpoints.external.port(),
                ),
                local,
                true,
            );
            let child = self.resources.spawn_process(request)?;
            state.pid = child.id();
            state.child = Some(child);
        }
        let mut startup = StartupAttempt(self.shared.clone(), true);
        let shared = self.shared.clone();
        let task_cancel = self.tasks.cancellation();
        self.tasks.spawn(TaskClass::Critical,async move {
            loop {tokio::select!{biased;
                _=task_cancel.cancelled()=>return Ok(()),
                _=shared.cancel.cancelled()=>return Ok(()),
                _=tokio::time::sleep(Duration::from_millis(50))=>if let Some(status)=shared.status()? {return Err(error(Stage::Receive,Reason::Io,"plugin child exited").with_source(ChildExited(status)));},
            }}
        }).map_err(|source|error(Stage::Prepare,Reason::Capacity,"plugin monitor refused").with_source(source))?;
        let deadline = deadline.min(Instant::now() + Duration::from_secs(10));
        let result = tokio::time::timeout_at(deadline, self.ready(deadline))
            .await
            .map_err(|source| {
                error(Stage::Prepare, Reason::Timeout, "plugin start timed out").with_source(source)
            })?;
        if result.is_ok() {
            startup.1 = false;
        }
        result
    }
    fn revoke(&self, _: CloseReason) {
        self.shared.stop();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.shared.stop();
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
