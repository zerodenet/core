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
    BoxFuture, CancellationToken, ChannelError, Reason,
    component::{GrantedDatagramListener, ProcessRequest, ResourceAccess, ResourceDeclaration},
    packet::BoxPacketSession,
};

pub struct HostResources {
    source_pause:
        std::sync::OnceLock<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>>,
    originals: Arc<crate::HostOriginalBudgets>,
    original_tasks: Option<zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
    packet_source: Arc<
        std::sync::OnceLock<
            zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        >,
    >,
    component: ComponentId,
    icmp: Option<super::icmp::HostIcmpProbeOwner>,
    coverage: Vec<coverage::CoverageBinding>,
    carriers: carrier::CarrierDeclarations,
    metadata: metadata::MetadataDeclarations,
    metapools: metapool::MetadataPools,
    roots: root_datagram::RootDeclarations,
    bounds: bound_datagram::BoundDeclarations,
    packets: Vec<packet_device::PacketDeviceOwner>,
    reads: read_workspaces::ReadDeclarations,
    #[cfg(any(target_os = "linux", windows))]
    processes: process::ProcessDeclarations,
    cancel: CancellationToken,
    state: Mutex<State>,
    changed: Arc<tokio::sync::Notify>,
    cleanup: AsyncMutex<()>,
}
impl HostResources {
    /// Install the same actual System TUN source metadata before factory build.
    pub fn install_packet_source_pause(
        &self,
        pause: Arc<zero_platform_tokio::contract::packet::PacketSourcePause>,
    ) -> Result<(), ChannelError> {
        if !self
            .original_tasks
            .as_ref()
            .is_some_and(|tasks| tasks.scope_identity().same_scope(pause.scope()))
            || !self
                .packets
                .iter()
                .any(|owner| owner.source_matches(pause.instance(), pause.scope()))
        {
            return Err(self.denied());
        }
        self.source_pause.set(pause).map_err(|_| self.denied())
    }

    pub fn original_packet_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::PacketProviderRegistration> {
        self.packets
            .iter()
            .find(|owner| owner.ordinal == ordinal)?
            .original_registration()
    }
    /// The actual source borrows this same ICMP provider; no second pool.
    pub fn icmp_probe_grant(&self) -> Option<super::icmp::HostIcmpProbeGrant> {
        self.icmp.as_ref().map(|owner| owner.grant())
    }
    pub(in crate::component) fn original_metadata_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolRegistration>
    {
        self.metapools.original_registration(ordinal)
    }
    pub(in crate::component) fn original_carrier_owner(
        &self,
        ordinal: usize,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration> {
        self.carriers.original_registration(ordinal)
    }
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
mod bound_datagram;
mod carrier;
mod coverage;
mod evidence;
mod files;
mod listener;
mod metadata;
mod metapool;
mod original_budgets;
mod packet_device;
mod prepare;
#[cfg(any(target_os = "linux", windows))]
mod process;
mod root_datagram;
pub use listener::HostDatagramListenerOwner;
mod read_workspaces;
mod shutdown;
mod sockets;
pub use read_workspaces::{HostPacketReadReservation, packet_datagram_read_spec};

/// Closing the grant phase does not claim that its children have exited.
pub struct StartAccess(Arc<HostResources>);
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
    pub fn activate(self: &Arc<Self>, deadline: Instant) -> Result<StartAccess, ChannelError> {
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
        drop(state);
        let access = StartAccess(self.clone());
        if let Some(icmp) = &self.icmp {
            icmp.activate(deadline)?;
        }
        self.roots.activate();
        Ok(access)
    }
    fn require_start(&self) -> Result<(), ChannelError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed
            || self.cancel.is_cancelled()
            || state
                .start_deadline
                .is_none_or(|deadline| Instant::now() >= deadline)
        {
            return Err(error(
                self.component,
                Reason::Revoked,
                "Packet native acquisition requires its bounded lifecycle start",
            ));
        }
        Ok(())
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
    fn packet_source_pause(
        &self,
    ) -> Option<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>> {
        self.source_pause.get().cloned()
    }

    #[cfg(feature = "raw-ip-runtime")]
    fn take_packet_datagram_reads(
        &self,
        datagram: usize,
    ) -> Result<[zero_tun::packet_memory::NativeReadGrant; 2], ChannelError> {
        self.require_start()?;
        self.reads.take(datagram)
    }
    fn packet_carrier_origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        if self.cancel.is_cancelled()
            || self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed
        {
            return Err(self.denied());
        }
        self.packet_source
            .get()
            .filter(|origin| !origin.is_retired())
            .cloned()
            .map(zero_platform_tokio::controlled_carrier::CarrierOrigin::from_listener)
            .ok_or_else(|| self.denied())
    }

    fn bound_datagram_retired(&self, ordinal: usize) -> bool {
        self.bounds.retired(ordinal)
    }
    fn activate_bound_datagram(
        &self,
        ordinal: usize,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<zero_platform_tokio::contract::component::DatagramBindingId, ChannelError> {
        self.require_start()?;
        let binding = self.bounds.activate(ordinal, driver)?;
        self.reads.prepare_driver(ordinal, driver)?;
        Ok(binding)
    }
    fn select_bound_outbound_datagram(
        &self,
        request: zero_platform_tokio::contract::component::BoundDatagramRequest,
    ) -> Result<zero_platform_tokio::EgressSelection, ChannelError> {
        self.bounds
            .owner_for(&request.binding)?
            .select_bound_outbound_datagram(request)
    }
    fn open_bound_outbound_datagram(
        &self,
        request: zero_platform_tokio::contract::component::BoundDatagramRequest,
    ) -> Result<zero_platform_tokio::contract::component::GrantedBoundDatagram, ChannelError> {
        self.bounds
            .owner_for(&request.binding)?
            .open_bound_outbound_datagram(request)
    }
    fn open_bound_datagram_attempt(
        &self,
        request: zero_platform_tokio::contract::component::BoundDatagramRequest,
    ) -> Result<
        zero_platform_tokio::contract::component::GrantedBoundDatagram,
        zero_platform_tokio::contract::component::BoundDatagramAttemptFailure,
    > {
        self.bounds
            .owner_for(&request.binding)
            .map_err(zero_platform_tokio::contract::component::BoundDatagramAttemptFailure::denied)?
            .open_bound_datagram_attempt(request)
    }

    fn bind_inbound_icmp(
        &self,
        source: std::net::IpAddr,
    ) -> Result<zero_platform_tokio::controlled_icmp::ControlledIcmpSocket, ChannelError> {
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed
                || self.cancel.is_cancelled()
                || state.start_deadline.is_none_or(|d| Instant::now() >= d)
            {
                return Err(error(
                    self.component,
                    Reason::Revoked,
                    "ICMP bind requires the actual bounded lifecycle start",
                ));
            }
        }
        self.icmp
            .as_ref()
            .ok_or_else(|| self.denied())?
            .grant()
            .bind(source, None)
    }

    fn register_metadata_pool(
        &self,
        ordinal: usize,
        actual: zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolRegistration,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), ChannelError> {
        self.register_metapool(ordinal, actual, tasks)
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt: &zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<(), ChannelError> {
        self.confirm_metadata_original_users(ordinal, receipt, tasks)
    }

    fn open_root_datagram(
        &self,
        request: &zero_platform_tokio::contract::component::RootDatagramRequest,
        query: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::contract::component::GrantedRootDatagram,
        zero_platform_tokio::contract::component::RootDatagramAttemptFailure,
    > {
        self.open_root(request, query)
    }
    fn close_root_datagram(
        &self,
        usage: zero_platform_tokio::contract::component::RootDatagramUse,
    ) -> Result<zero_platform_tokio::contract::component::RootDatagramClosed, ChannelError> {
        self.close_root(usage)
    }

    fn listen_metadata_datagram(
        &self,
        request: zero_platform_tokio::contract::component::MetadataDatagramRequest,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedMetadataListener, ChannelError>,
    > {
        Box::pin(async move { self.bind_metadata(request) })
    }
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
        #[cfg(any(target_os = "linux", windows))]
        self.processes.mark_legacy(&request.program);
        *remaining -= 1;
        Ok(child)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn prepare_owned_process(
        &self,
        request: ProcessRequest,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, ChannelError> {
        self.prepare_process(request, tasks)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn process_endpoint_retirement(
        &self,
        ordinal: usize,
    ) -> Option<Arc<dyn zero_platform_tokio::owned_process::ProcessEndpointRetirement>> {
        self.processes.endpoint_retirement(ordinal)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn reserve_process_endpoint(
        &self,
        ordinal: usize,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
        actor: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<Arc<zero_platform_tokio::owned_process::ProcessEndpointHandoff>, ChannelError> {
        self.reserve_endpoint(ordinal, tasks, actor)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn prepare_owned_process_with_endpoint(
        &self,
        request: ProcessRequest,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
        endpoint: &Arc<zero_platform_tokio::owned_process::ProcessEndpointHandoff>,
    ) -> Result<Arc<zero_platform_tokio::owned_process::OwnedProcess>, ChannelError> {
        self.prepare_process_with_endpoint(request, tasks, endpoint)
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
    fn open_packet_device(
        &self,
        name: Arc<str>,
        driver: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedPacketDevice, ChannelError>,
    > {
        Box::pin(async move {
            self.require_start()?;
            self.packets
                .iter()
                .find(|owner| owner.name() == name.as_ref())
                .ok_or_else(|| self.denied())?
                .open(driver)
                .await
        })
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

impl HostResources {
    pub(in crate::component) fn poll_cancelled_carriers(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> bool {
        #[cfg(any(target_os = "linux", windows))]
        self.processes.poll_budget(cx);
        self.carriers.poll_cancelled(cx)
    }
}
