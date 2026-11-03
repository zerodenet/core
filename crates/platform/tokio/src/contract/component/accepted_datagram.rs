//! Host-selected accepted UDP originals. This grants no unrelated bind or task.
use super::*;
use crate::contract::{
    lifecycle::{erase_lifecycle, CloseReport, ResourceLifecycle, Unfinished},
    scope::TaskScopeIdentity,
    ChannelError, CloseReason, Impact, Reason, Stage,
};
use crate::{
    controlled_datagram::{listener::ControlledDatagramListenerControl, ControlledDatagramOwner},
    AcceptedCaller,
};
use std::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramPeerLimits {
    pub max_tasks_per_peer: NonZeroUsize,
    pub max_resources_per_peer: usize,
    pub max_peers: NonZeroUsize,
    pub packets_per_peer: NonZeroUsize,
}

impl DatagramPeerLimits {
    pub fn task_slots(self) -> Option<NonZeroUsize> {
        self.max_peers
            .get()
            .checked_mul(self.max_tasks_per_peer.get())
            .and_then(NonZeroUsize::new)
    }
}

pub struct AcceptedDatagramAuthority {
    scope: TaskScopeIdentity,
    cancel: CancellationToken,
    sockets: NonZeroUsize,
    sends: NonZeroUsize,
}
struct Original {
    native: ControlledDatagramOwner,
    control: Option<ControlledDatagramListenerControl>,
}
struct Owner(Arc<Mutex<Original>>);
impl AcceptedDatagramAuthority {
    /// The real host supplies its selected declaration and original Scope.
    pub fn new(
        declaration: &ResourceDeclaration,
        registrar: &TaskRegistrar,
        cancel: CancellationToken,
    ) -> Result<Self, BuildError> {
        let ResourceDeclaration::AcceptedDatagram {
            max_sockets,
            max_pending_sends,
        } = declaration
        else {
            return Err(denied());
        };
        Ok(Self {
            scope: registrar.scope_identity(),
            cancel,
            sockets: *max_sockets,
            sends: *max_pending_sends,
        })
    }
    pub fn open(&self, caller: &AcceptedCaller) -> Result<GrantedDatagramListener, BuildError> {
        caller.check_open()?;
        if self.cancel.is_cancelled() || !self.scope.same_scope(&caller.scope_identity()) {
            return Err(denied());
        }
        let local = caller.local_endpoint()?;
        let original = Arc::new(Mutex::new(Original {
            native: ControlledDatagramOwner::new(self.sockets),
            control: None,
        }));
        // The actual accepted entry holds this owner BEFORE synchronous native
        // acquisition. Failed binding or a dropped success reply cannot lose it.
        caller.retain_resource(0, erase_lifecycle(Owner(original.clone())))?;
        let mut owner = original
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (rx, tx, control) = owner
            .native
            .bind_listener(std::net::SocketAddr::new(local.ip(), 0), self.sends)
            .map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Session).with_source(source)
            })?;
        owner.control = Some(control);
        Ok(GrantedDatagramListener { rx, tx })
    }
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(control) = &self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .control
        {
            control.revoke();
        }
    }
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        let mut original = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(control) = original.control.as_mut() {
            let _ = control.close();
        }
        let native = original.native.close_all();
        let mut report = CloseReport::default();
        if native.confirmed_closed != 0 {
            report.closed_io.push(format!(
                "{} original accepted UDP sockets closed",
                native.confirmed_closed
            ));
        }
        if !native.is_complete() {
            report.unfinished.push(Unfinished::Io(
                "original accepted UDP socket closure remains unconfirmed".into(),
            ));
        }
        // Unknown reservations and their original sources remain in native;
        // they cannot be replaced by a new bind or a port-availability probe.
        Ok(report)
    }
}
fn denied() -> BuildError {
    ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Session)
}
