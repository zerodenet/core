//! Persistent ownership of the actual inbound plugin Child.
use super::process;
use crate::validation::PluginConfig;
use std::{
    io,
    net::SocketAddr,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    process::Child,
    time::{sleep_until, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{BuildError, CloseReport, ResourceLifecycle},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

#[derive(Default)]
pub(super) struct State {
    started: bool,
    activated: bool,
    child: Option<Child>,
    pid: Option<u32>,
    exited: Option<ExitStatus>,
}
#[derive(Default)]
pub(super) struct Shared {
    state: Mutex<State>,
    pub(super) cancel: CancellationToken,
}
fn stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "inbound plugin owner is closing",
    )
}
impl Shared {
    pub(super) fn begin(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if self.cancel.is_cancelled() {
            return Err(stopped());
        }
        if !state.started || state.activated {
            return Err(io::Error::other(
                "inbound plugin activation requires its installed owner and can run only once",
            ));
        }
        state.activated = true;
        Ok(())
    }
    pub(super) fn spawn(
        &self,
        config: &PluginConfig,
        remote: (&str, u16),
        local: SocketAddr,
    ) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if self.cancel.is_cancelled() {
            return Err(stopped());
        }
        // The closing check, actual spawn and persistent installation are one
        // synchronous critical section. Shutdown cannot confirm before a late spawn.
        let child = process::spawn(config, remote, local, true)?;
        state.pid = child.id();
        state.child = Some(child);
        Ok(())
    }
    pub(super) fn status(&self) -> io::Result<Option<ExitStatus>> {
        let mut state = self.state.lock().unwrap();
        if let Some(status) = state.exited {
            return Ok(Some(status));
        }
        let status = state
            .child
            .as_mut()
            .ok_or_else(|| io::Error::other("inbound plugin child was not started"))?
            .try_wait()?;
        state.exited = status;
        Ok(status)
    }
    pub(super) fn request_stop(&self) {
        self.cancel.cancel();
        let mut state = self.state.lock().unwrap();
        if let Some(child) = state.child.as_mut() {
            let _ = child.start_kill();
        }
    }
    async fn shutdown(&self, deadline: Instant) -> io::Result<Option<(u32, ExitStatus)>> {
        self.cancel.cancel();
        {
            let mut state = self.state.lock().unwrap();
            if state.child.is_none() {
                return Ok(None);
            }
            if state.exited.is_none() {
                let child = state.child.as_mut().unwrap();
                if let Some(status) = child.try_wait()? {
                    state.exited = Some(status);
                } else {
                    child.start_kill()?;
                }
            }
        }
        loop {
            if let Some(status) = self.status()? {
                return Ok(Some((
                    self.state.lock().unwrap().pid.expect("spawned child PID"),
                    status,
                )));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "inbound plugin child exit is not confirmed",
                ));
            }
            sleep_until(deadline.min(now + Duration::from_millis(10))).await;
        }
    }
}
pub(super) struct Owner(pub(super) Arc<Shared>);
fn failure(source: io::Error, stage: Stage) -> BuildError {
    let reason = match source.kind() {
        io::ErrorKind::TimedOut => Reason::Timeout,
        io::ErrorKind::NotConnected => Reason::Revoked,
        _ => Reason::Io,
    };
    ChannelError::new(stage, reason, Impact::Component).with_source(source)
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let mut state = self.0.state.lock().unwrap();
        if self.0.cancel.is_cancelled() {
            return Err(failure(stopped(), Stage::Prepare));
        }
        if Instant::now() >= deadline {
            return Err(failure(
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "inbound owner start deadline elapsed",
                ),
                Stage::Prepare,
            ));
        }
        state.started = true;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.0.request_stop();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        let actual = self
            .0
            .shutdown(deadline)
            .await
            .map_err(|source| failure(source, Stage::Close))?;
        let mut report = CloseReport::default();
        if let Some((pid, status)) = actual {
            report.released_resources.push(format!(
                "shadowsocks inbound plugin child {pid} reaped: {status}"
            ));
        }
        // No claim about listener, UDP socket, descendants or transferred I/O.
        Ok(report)
    }
}
pub(super) struct StartupAttempt {
    pub(super) shared: Arc<Shared>,
    pub(super) armed: bool,
}
impl Drop for StartupAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.shared.request_stop();
        }
    }
}
