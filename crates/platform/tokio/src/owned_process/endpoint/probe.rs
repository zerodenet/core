//! One inline probe at a time; its native receipt never joins its own actor.
use super::*;
use crate::{
    ControlledTcpControl, ControlledTcpStream, PendingTcpConnect,
    contract::lifecycle::ChannelControl,
};
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
pub struct ProcessProbe {
    io: Option<ControlledTcpStream>,
    original: Arc<Original>,
}
pub struct ProcessProbeClosed {
    original: Weak<Original>,
    had_native_acquisition: bool,
}
impl ProcessProbeClosed {
    #[cfg(test)]
    pub(super) fn matches_original(&self, original: &Arc<Original>) -> bool {
        self.original.ptr_eq(&Arc::downgrade(original))
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.had_native_acquisition
    }
    pub fn matches(&self, probe: &ProcessProbe) -> bool {
        self.original.ptr_eq(&Arc::downgrade(&probe.original))
    }
}
pub(super) struct Original {
    running: AtomicBool,
    view: AtomicBool,
    stop: CancellationToken,
    control: Mutex<Option<Arc<AsyncMutex<ControlledTcpControl>>>>,
    unknown: Mutex<Option<Arc<ChannelError>>>,
    closed: OnceLock<Arc<ProcessProbeClosed>>,
}
impl Original {
    fn new(cancel: &CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            running: AtomicBool::new(true),
            view: AtomicBool::new(false),
            stop: cancel.child_token(),
            control: Mutex::new(None),
            unknown: Mutex::new(None),
            closed: OnceLock::new(),
        })
    }
    pub(super) fn stop(&self) {
        self.stop.cancel();
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed.get().is_some()
    }
    pub(super) async fn close(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<Option<Arc<ProcessProbeClosed>>, ChannelError> {
        self.stop();
        if let Some(closed) = self.closed.get() {
            return Ok(Some(closed.clone()));
        }
        if self.running.load(Ordering::Acquire) || self.view.load(Ordering::Acquire) {
            return Ok(None);
        }
        let unknown = { self.unknown.lock().unwrap().clone() };
        if let Some(unknown) = unknown {
            return Err(channel(unknown));
        }
        let control = { self.control.lock().unwrap().clone() };
        let had_native_acquisition = control.is_some();
        if let Some(control) = control {
            let mut control = tokio::time::timeout_at(deadline, control.lock())
                .await
                .map_err(|_| {
                    channel(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "actual probe close gate pending",
                    ))
                })?;
            let report = control.wait_closed(deadline).await?;
            if !report.is_complete() {
                return Ok(None);
            }
            // Exact original's state, not an empty/previously consumed report.
            if !control
                .close_observer()
                .observe()
                .is_some_and(|observation| observation.closed)
            {
                return Ok(None);
            }
        }
        let _ = self.closed.set(Arc::new(ProcessProbeClosed {
            original: Arc::downgrade(self),
            had_native_acquisition,
        }));
        Ok(self.closed.get().cloned())
    }
}
struct Running(Arc<Original>);
impl Drop for Running {
    fn drop(&mut self) {
        self.0.running.store(false, Ordering::Release);
    }
}
impl ProcessEndpointHandoff {
    /// Only the exact started job's actor can perform this bounded background IO.
    /// The caller's existing actor executes connect inline, without spawning.
    pub async fn open_probe(
        self: &Arc<Self>,
        actor: &TaskJoinWitness,
        deadline: Instant,
    ) -> Result<ProcessProbe, ChannelError> {
        let process = self.live_process().map_err(channel)?;
        if !self.spec.stream
            || !self.scope.same_scope(&actor.scope_identity())
            || actor.is_joined()
            || !process
                .actor()
                .is_some_and(|actual| actual.same_registration(actor))
        {
            return Err(channel(denied(
                "probe is not the same live process actor/network",
            )));
        }
        let address = self.address().map_err(channel)?;
        let original = Original::new(&self.cancel);
        {
            let mut state = self.lock();
            if state.stopping || state.probe.as_ref().is_some_and(|old| !old.is_closed()) {
                return Err(channel(denied(
                    "previous actual probe still retains native IO or view",
                )));
            }
            // Persist before preparation can acquire a native fd or await.
            state.probe = Some(original.clone());
        }
        let _running = Running(original.clone());
        let selection =
            crate::egress::process_endpoint_selection(address, self.spec.network_generation)
                .map_err(channel)?;
        let policy = zero_traits::DialPolicy::default();
        *original.unknown.lock().unwrap() = Some(Arc::new(channel(denied(
            "probe preparation outcome is unconfirmed",
        ))));
        let (mut pending, control) = match PendingTcpConnect::prepare(
            address,
            &policy,
            &selection,
            original.stop.clone(),
            None,
        ) {
            Ok(value) => value,
            Err(error) => {
                let known_no_native = error.stage() == "validate_dial_policy";
                let error = Arc::new(channel(error));
                *original.unknown.lock().unwrap() = (!known_no_native).then(|| error.clone());
                return Err(channel(error));
            }
        };
        *original.control.lock().unwrap() = Some(Arc::new(AsyncMutex::new(control)));
        *original.unknown.lock().unwrap() = None;
        let io = pending.connect(deadline).await.map_err(channel)?;
        if self.cancel.is_cancelled() {
            drop(io);
            return Err(channel(denied("probe revoked during connect")));
        }
        if io.peer_addr().map_err(channel)? != address
            || io.local_addr().map_err(channel)? == address
        {
            drop(io);
            return Err(channel(denied(
                "probe original has incorrect actual endpoints",
            )));
        }
        original.view.store(true, Ordering::Release);
        Ok(ProcessProbe {
            io: Some(io),
            original,
        })
    }
    /// Progress a failed/cancelled inline attempt before retrying; never actor.join.
    pub async fn close_probe(
        &self,
        deadline: Instant,
    ) -> Result<Option<Arc<ProcessProbeClosed>>, ChannelError> {
        let probe = { self.lock().probe.clone() };
        match probe {
            Some(probe) => probe.close(deadline).await,
            None => Ok(None),
        }
    }
}
impl ProcessProbe {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io
            .as_ref()
            .ok_or_else(|| denied("probe view returned"))?
            .local_addr()
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io
            .as_ref()
            .ok_or_else(|| denied("probe view returned"))?
            .peer_addr()
    }
    pub async fn close(
        &mut self,
        deadline: Instant,
    ) -> Result<Option<Arc<ProcessProbeClosed>>, ChannelError> {
        drop(self.io.take());
        self.original.view.store(false, Ordering::Release);
        self.original.close(deadline).await
    }
}
impl Drop for ProcessProbe {
    fn drop(&mut self) {
        drop(self.io.take());
        self.original.view.store(false, Ordering::Release);
        self.original.stop.cancel();
    }
}
