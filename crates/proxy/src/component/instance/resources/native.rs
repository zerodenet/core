//! Declared native starts; the protocol lifecycle retains the actual Child.
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::Instant;
use zero_core::{component::ComponentId, request::Destination};
use zero_platform_tokio::contract::{
    component::{ProcessRequest, ResourceAccess, ResourceDeclaration},
    packet::BoxPacketSession,
    BoxFuture, CancellationToken, ChannelError, Reason,
};

pub(in crate::component) struct HostResources {
    component: ComponentId,
    coverage: Vec<coverage::CoverageBinding>,
    carriers: carrier::CarrierDeclarations,
    cancel: CancellationToken,
    state: Mutex<State>,
    changed: Arc<tokio::sync::Notify>,
    cleanup: AsyncMutex<()>,
}
struct State {
    closed: bool,
    start_deadline: Option<Instant>,
    remaining: BTreeMap<PathBuf, usize>,
    tcp: BTreeMap<SocketAddr, Arc<AsyncMutex<sockets::TcpGrant>>>,
    datagram: BTreeMap<SocketAddr, Arc<AsyncMutex<sockets::DatagramGrant>>>,
    files: files::FileReads,
    io: shutdown::NativeClose,
}
// The nonconsuming proof API is staged for host slot integration; resourceful
// production activation remains gated until that separate transaction exists.
mod carrier;
mod coverage;
mod evidence;
mod files;
mod listener;
mod prepare;
pub(in crate::component) use evidence::HostIoEvidence;
pub(crate) use listener::HostDatagramListenerOwner;
mod shutdown;
mod sockets;

/// Closing the grant phase does not claim that its children have exited.
pub(in crate::component) struct StartAccess(Arc<HostResources>);
impl Drop for StartAccess {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .start_deadline = None;
    }
}
impl HostResources {
    pub(in crate::component) fn activate(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<StartAccess, ChannelError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "native resource owner was revoked",
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "native resource start deadline elapsed",
            ));
        }
        state.start_deadline = Some(deadline);
        Ok(StartAccess(self.clone()))
    }
    fn denied(&self) -> ChannelError {
        error(
            self.component,
            Reason::InvalidConfig,
            "undeclared resource access",
        )
    }
}
impl ResourceAccess for HostResources {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        registration: zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOwnerBinding, ChannelError> {
        self.register_carrier(ordinal, registration, tasks)
    }
    fn spawn_process(
        &self,
        request: ProcessRequest,
    ) -> Result<tokio::process::Child, ChannelError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "native resource owner was revoked",
            ));
        }
        let Some(deadline) = state.start_deadline else {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "native starts require the lifecycle start phase",
            ));
        };
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "native resource start deadline elapsed",
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
                "declared process start budget exhausted",
            ));
        }
        let child = request.command().spawn().map_err(|source| {
            error(self.component, Reason::Io, "native process spawn failed").with_source(source)
        })?;
        *remaining -= 1;
        Ok(child)
    }
    fn read_file(
        &self,
        path: PathBuf,
        max_bytes: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(self.read_declared_file(path, max_bytes))
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async move { self.bind_tcp(address) })
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, ChannelError>,
    > {
        Box::pin(async move { self.bind_datagram(address) })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(self.denied()) })
    }
}
fn error(component: ComponentId, reason: Reason, message: &'static str) -> ChannelError {
    let mut error = crate::component::instance::invalid(reason, message);
    error.component = Some(component);
    error
}
