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
    component::{GrantedDatagramListener, ProcessRequest, ResourceAccess, ResourceDeclaration},
    packet::BoxPacketSession,
    BoxFuture, CancellationToken, ChannelError, Reason,
};

pub struct HostResources {
    packet_failures: zero_platform_tokio::contract::lifecycle::ResourceCleanupFailures,
    packet_calls:
        std::sync::OnceLock<Arc<zero_platform_tokio::contract::component::PacketDriverCalls>>,
    node_dns_source: std::sync::OnceLock<
        Arc<std::sync::OnceLock<Arc<dyn zero_platform_tokio::contract::dns::DnsService>>>,
    >,
    dependency_slots:
        std::sync::OnceLock<Arc<[std::sync::Weak<crate::component::instance::ComponentSlot>]>>,
    source_pause:
        std::sync::OnceLock<Arc<zero_platform_tokio::contract::packet::PacketSourcePause>>,
    originals: Arc<crate::HostOriginalBudgets>,
    original_tasks: Option<zero_platform_tokio::contract::scope::WeakTaskRegistrar>,
    packet_source: Arc<std::sync::OnceLock<zero_platform_tokio::controlled_carrier::CarrierOrigin>>,
    component: ComponentId,
    instance: Option<zero_core::component::ComponentInstanceId>,
    icmp: Option<super::icmp::HostIcmpProbeOwner>,
    coverage: Vec<coverage::CoverageBinding>,
    carriers: carrier::CarrierDeclarations,
    metadata: metadata::MetadataDeclarations,
    metapools: metapool::MetadataPools,
    roots: root_datagram::RootDeclarations,
    bounds: bound_datagram::BoundDeclarations,
    packets: Vec<packet_device::PacketDeviceOwner>,
    reads: read_workspaces::ReadDeclarations,
    journals: journal::Journals,
    declared_failures: Arc<failures::DeclaredReceiver>,
    protocol_failures: Arc<failures::DeclaredReceiver>,
    protocol_supervision:
        std::sync::OnceLock<std::sync::Weak<crate::component::admission::HostAdmission>>,
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
mod dependency;
mod evidence;
mod failures;
pub(in crate::component) use failures::DeclaredReceiver;
mod files;
mod journal;
mod listener;
mod metadata;
mod metapool;
mod node_dns;
mod original_budgets;
mod packet_device;
mod packet_driver;
mod prepare;
#[cfg(any(target_os = "linux", windows))]
mod process;
mod protocol;
mod root_datagram;
pub use listener::HostDatagramListenerOwner;
mod read_workspaces;
mod shutdown;
mod sockets;
pub use read_workspaces::{
    packet_datagram_control_spec, packet_datagram_read_spec, packet_datagram_supplemental_spec,
    HostPacketReadReservation,
};

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
    fn receive_packet_driver_failure(
        &self,
        grant: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
        source: Arc<ChannelError>,
    ) -> Result<bool, ChannelError> {
        if self.instance != Some(grant.original().instance())
            || !self
                .packet_calls
                .get()
                .is_some_and(|actual| grant.matches_calls(actual))
            || !self.original_tasks.as_ref().is_some_and(|tasks| {
                tasks
                    .scope_identity()
                    .same_scope(&grant.caller().scope_identity())
            })
            || !grant.matches_cleanup_failure(&source)
        {
            return Err(self.denied());
        }
        // The observer remains valid for this same retained retiring owner;
        // accepting an error supplies no renewed role or IO liveness.
        Ok(self.packet_failures.try_receive(
            source,
            zero_platform_tokio::contract::lifecycle::CleanupFailurePhase::Driver,
        ))
    }

    fn packet_driver_caller(
        &self,
        original: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
        driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        binding: zero_platform_tokio::contract::network::UpstreamBinding,
        call: zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<zero_platform_tokio::contract::component::GrantedPacketServiceCaller, ChannelError>
    {
        self.issue_packet_driver_caller(original, driver, binding, call)
    }
    fn service_datagram_target_issuer(
        &self,
        grant: &zero_platform_tokio::controlled_carrier::CarrierGrant,
        actor: &zero_platform_tokio::contract::scope::TaskJoinWitness,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        endpoint: &Destination,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> Result<
        zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetIssuer,
        ChannelError,
    > {
        if self.cancel.is_cancelled() || self.instance != Some(grant.instance()) {
            return Err(self.denied());
        }
        let bound = self.carriers.registered_dependency_receiver(grant)?;
        {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed || state.start_deadline.is_some() {
                return Err(self.denied());
            }
        }
        bound.service_datagram_target_issuer(
            self.carriers.host_fence()?,
            grant,
            actor,
            binding,
            endpoint,
            identity,
            call,
        )
    }

    fn journal(
        &self,
        ordinal: usize,
    ) -> Result<zero_platform_tokio::journal::JournalGrant, ChannelError> {
        self.require_start()?;
        self.journals.open(self.component, ordinal)
    }

    fn receive_protocol_report(
        &self,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
        report: &mut zero_platform_tokio::contract::lifecycle::CloseReport,
    ) -> Result<Option<Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>>, ChannelError>
    {
        self.receive_protocol_close_report(scope, report)
    }

    fn receive_protocol_failure(
        &self,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
        source: &Arc<ChannelError>,
    ) -> Result<Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>, ChannelError> {
        self.receive_protocol_fault(scope, source)
    }

    fn declared_resource_sources(
        &self,
    ) -> Option<zero_platform_tokio::contract::lifecycle::DeclaredResourceSources> {
        Some(self.declared_failures.sources(self.instance?))
    }

    fn packet_driver_metadata(
        &self,
    ) -> Result<zero_platform_tokio::contract::component::PacketDriverMetadataView, ChannelError>
    {
        Ok(self.originals.packet.driver_metadata_view())
    }

    fn carrier_buffer_budget(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierBufferBudget> {
        Some(self.originals.carrier.buffer_budget())
    }
    fn dependency_stream_loan_issuer(
        &self,
        dependency: zero_core::component::ComponentInstanceId,
        endpoint: &Destination,
        binding: &zero_platform_tokio::contract::network::UpstreamBinding,
        grant: &zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> Result<
        zero_platform_tokio::controlled_carrier::loan::CarrierDependencyLoanIssuer,
        ChannelError,
    > {
        self.registered_dependency_stream_issuer(dependency, endpoint, binding, grant)
    }

    fn prepare_logical_packet_source(
        &self,
        source: &zero_platform_tokio::controlled_carrier::LogicalPacketProviderRegistration,
    ) -> Result<(), ChannelError> {
        self.require_start()?;
        if self.instance != Some(source.instance()) || source.is_retired() {
            return Err(self.denied());
        }
        source.reserve_original_totals(
            self.original_tasks.as_ref().ok_or_else(|| self.denied())?,
            &self.originals.carrier,
        )?;
        self.prepare_packet_driver_calls(source)
    }
    fn install_logical_packet_source(
        &self,
        source: zero_platform_tokio::controlled_carrier::LogicalPacketProviderRegistration,
    ) -> Result<(), ChannelError> {
        self.require_start()?;
        if self.instance != Some(source.instance())
            || source.is_retired()
            || !source.has_original_total(&self.originals.carrier)
            || !self
                .original_tasks
                .as_ref()
                .is_some_and(|tasks| tasks.scope_identity().same_scope(source.device().scope()))
        {
            return Err(self.denied());
        }
        let source =
            zero_platform_tokio::controlled_carrier::CarrierOrigin::from_logical_packet(source);
        if let Some(actual) = self.packet_source.get() {
            return if actual.same_original(&source) {
                Ok(())
            } else {
                Err(self.denied())
            };
        }
        self.packet_source.set(source).map_err(|_| self.denied())
    }
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
    #[cfg(feature = "raw-ip-runtime")]
    fn take_packet_datagram_receive_workspace(
        &self,
        datagram: usize,
        reader: usize,
    ) -> Result<zero_tun::packet_memory::NativeReadGrant, ChannelError> {
        self.require_start()?;
        self.reads.take_receive(datagram, reader)
    }
    #[cfg(feature = "raw-ip-runtime")]
    fn take_packet_datagram_control(
        &self,
        datagram: usize,
    ) -> Result<Option<zero_tun::packet_memory::NativeReadGrant>, ChannelError> {
        self.require_start()?;
        self.reads.take_control(datagram)
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

#[cfg(test)]
#[path = "../../tests/resource_journal.rs"]
mod journal_tests;
