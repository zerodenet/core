//! Declared native starts; the protocol lifecycle retains the actual Child.
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::time::Instant;
use zero_core::{component::ComponentId, request::Destination};
use zero_platform_tokio::contract::{
    component::{ProcessRequest, ResourceAccess, ResourceDeclaration},
    packet::BoxPacketSession,
    BoxFuture, CancellationToken, ChannelError, Reason,
};

pub(in crate::component) struct HostResources {
    component: ComponentId,
    cancel: CancellationToken,
    state: Mutex<State>,
}
struct State {
    start_deadline: Option<Instant>,
    remaining: BTreeMap<PathBuf, usize>,
    tcp: BTreeMap<SocketAddr, bool>,
    datagram: BTreeMap<SocketAddr, bool>,
}
mod sockets;

/// Closing the grant phase does not claim that its children have exited.
pub(in crate::component) struct StartAccess(Arc<HostResources>);
impl Drop for StartAccess {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("native grants poisoned")
            .start_deadline = None;
    }
}
impl HostResources {
    pub(in crate::component) fn new(
        component: ComponentId,
        declarations: &[ResourceDeclaration],
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, ChannelError> {
        let mut remaining = BTreeMap::new();
        let mut tcp = BTreeMap::new();
        let mut datagram = BTreeMap::new();
        for declaration in declarations {
            match declaration {
                ResourceDeclaration::Process {
                    program,
                    max_starts,
                } => {
                    if program.as_os_str().is_empty()
                        || remaining
                            .insert(program.clone(), max_starts.get())
                            .is_some()
                    {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "empty or duplicate process declaration",
                        ));
                    }
                }
                ResourceDeclaration::TcpListener(address)
                | ResourceDeclaration::DatagramListener(address) => {
                    let address = sockets::literal(component, address)?;
                    let grants = if matches!(declaration, ResourceDeclaration::TcpListener(_)) {
                        &mut tcp
                    } else {
                        &mut datagram
                    };
                    if grants.insert(address, false).is_some() {
                        return Err(error(
                            component,
                            Reason::InvalidConfig,
                            "duplicate listener declaration",
                        ));
                    }
                }
                _ => {
                    return Err(error(
                        component,
                        Reason::InvalidConfig,
                        "resource grant is not implemented",
                    ))
                }
            }
        }
        Ok(Arc::new(Self {
            component,
            cancel,
            state: Mutex::new(State {
                start_deadline: None,
                remaining,
                tcp,
                datagram,
            }),
        }))
    }
    pub(in crate::component) fn activate(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<StartAccess, ChannelError> {
        let mut state = self.state.lock().expect("native grants poisoned");
        if self.cancel.is_cancelled() {
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
    fn spawn_process(
        &self,
        request: ProcessRequest,
    ) -> Result<tokio::process::Child, ChannelError> {
        let mut state = self.state.lock().expect("native grants poisoned");
        if self.cancel.is_cancelled() {
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
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(self.denied()) })
    }
    fn listen_tcp(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::TcpListener, ChannelError>> {
        Box::pin(async move { self.bind_tcp(address) })
    }
    fn listen_datagram(
        &self,
        address: Destination,
    ) -> BoxFuture<'_, Result<tokio::net::UdpSocket, ChannelError>> {
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
