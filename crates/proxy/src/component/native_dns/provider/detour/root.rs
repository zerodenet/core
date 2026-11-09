//! A standard Stream role receives I/O owned by this actual DNS host.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{lifecycle::DynChannelControl, stream::BoxStream, ChannelError, CloseReason},
    controlled_carrier::{CarrierGrants, ResolvingCarrierOpener},
};
mod controlled;
mod direct;
mod native_datagram;
mod pending;
#[cfg(any(target_os = "linux", windows))]
mod process;
mod protocol;
pub(super) struct Root {
    receiver: super::super::failure::Receiver,
    query_entry: std::sync::OnceLock<std::sync::Weak<custody::NativeUse>>,
    native: Option<ResolvingCarrierOpener>,
    native_datagram: Option<(zero_core::component::ComponentInstanceId, usize)>,
    binding: UpstreamBinding,
    endpoint: Destination,
    node_dns: Arc<dyn zero_platform_tokio::contract::dns::DnsService>,
    egress: Arc<zero_platform_tokio::EgressInterfaceControl>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pending: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
    grants: CarrierGrants,
    budget: Arc<crate::component::native_dns::Budget>,
    users: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
    caller: TaskJoinWitness,
    cancel: CancellationToken,
    original: Mutex<Option<Arc<custody::NativeUse>>>,
    current: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
    roles: Vec<zero_core::component::RoleInstanceId>,
    candidates: tokio::sync::Mutex<Option<controlled::Candidates>>,
    invocation: CarrierContext,
    direct_endpoint: Option<Destination>,
    waiting: std::sync::OnceLock<zero_platform_tokio::contract::network::PendingCarrierObserver>,
}
impl Root {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        services: crate::protocol_registry::UpstreamConnectServices,
        endpoint: Destination,
        binding: UpstreamBinding,
        grants: CarrierGrants,
        budget: Arc<crate::component::native_dns::Budget>,
        users: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
        pending: Arc<Mutex<Vec<Arc<custody::NativeUse>>>>,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        invocation: CarrierContext,
        current: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
        roles: Vec<zero_core::component::RoleInstanceId>,
        receiver: super::super::failure::Receiver,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            receiver,
            query_entry: std::sync::OnceLock::new(),
            native: Some(ResolvingCarrierOpener::new(
                grants.registration().instance(),
                binding.clone(),
                endpoint.clone(),
                services.native_node_dns(),
                Arc::new(services.egress_control()),
                services.observer(),
            )?),
            native_datagram: None,
            binding,
            endpoint,
            node_dns: services.native_node_dns(),
            egress: Arc::new(services.egress_control()),
            observer: services.observer(),
            pending,
            grants,
            budget,
            users,
            caller,
            cancel,
            original: Mutex::default(),
            current,
            roles,
            candidates: Default::default(),
            invocation,
            direct_endpoint: None,
            waiting: Default::default(),
        })
    }
    /// Publish only a weak observer of this same query ledger. The temporary
    /// transport neither opens I/O nor registers another owner or task.
    pub(super) fn track(self: &Arc<Self>) {
        let waiting =
            UpstreamTransport::new(self.binding.clone(), self.clone()).pending_carrier_observer();
        assert!(
            self.waiting.set(waiting).is_ok(),
            "one observer per actual DNS root"
        );
    }
    /// A device donor needs the same actual pending ledger, but no
    /// authorization to dial a second native Stream behind that device.
    pub(super) fn observer_only(mut self) -> Self {
        self.native = None;
        self
    }
    fn check_pending(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let fail = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "DNS pending carrier differs from its original current role/binding",
            )
        };
        if binding != &self.binding
            || self.egress.generation() != binding.network_generation.0
            || self.cancel.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || self
                .grants
                .registration()
                .bound()
                .is_none_or(|bound| bound.host_cancelled())
        {
            return Err(fail());
        }
        let source = self.current.upgrade().ok_or_else(fail)?;
        let services = source.runtime_services().map_err(|_| fail())?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|_| fail())?;
        if !self
            .roles
            .iter()
            .any(|role| plan.validate_carrier_adoption(*role, adoption).is_ok())
        {
            return Err(fail());
        }
        Ok(())
    }

    pub(super) async fn loan(&self, io: BoxStream) -> std::io::Result<StreamLoan> {
        let entry = self
            .original
            .lock()
            .expect("DNS original poisoned")
            .clone()
            .ok_or_else(|| {
                std::io::Error::other("standard DNS Stream did not open its declared original")
            })?;
        let original = entry
            .logical
            .lock()
            .await
            .as_ref()
            .expect("original retained before protocol handshake")
            .original()
            .clone();
        Ok(StreamLoan::new(
            io,
            Box::new(Control(entry)).into(),
            original,
        ))
    }
}
struct Control(Arc<custody::NativeUse>);
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        // The unique outer view is revoked by LoanOwner. Custody must return
        // before the retained original may be retired.
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move { self.0.settle(end).await.map_err(error) })
    }
}
impl UpstreamOpener for Root {
    fn protocol_original_observer(
        &self,
    ) -> Option<zero_platform_tokio::contract::network::PendingCarrierObserver> {
        self.waiting.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        self.retain_query_protocol(binding, original)
    }

    fn open_controlled_stream_loan(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            self.direct_transport(&binding)?
                .open_controlled_stream_loan(request)
                .await
        })
    }
    fn open_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            self.direct_transport(&binding)?
                .open_controlled_stream(request)
                .await
        })
    }
    #[cfg(any(target_os = "linux", windows))]
    fn acquire_process_endpoint_stream(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ProcessEndpointStreamRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.process_stream(binding.clone(), request)
    }
    #[cfg(any(target_os = "linux", windows))]
    fn acquire_process_endpoint_datagram(
        &self,
        binding: &UpstreamBinding,
        request: zero_platform_tokio::contract::network::ProcessEndpointDatagramRequest,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierDatagram, ChannelError>>
    {
        self.process_datagram(binding.clone(), request)
    }

    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        let failure = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "Packet caller differs from its actual DNS query source",
            )
        };
        if binding != &self.binding
            || call.session.is_some()
            || call.cancel.is_cancelled()
            || self.cancel.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || self.egress.generation() != binding.network_generation.0
            || !matches!(identity, zero_platform_tokio::contract::context::RequestIdentity::Service(facts)
                if facts.origin == zero_core::request::ServiceOrigin::Runtime
                && facts.purpose == zero_core::request::ServicePurpose::DnsQuery)
            || self
                .grants
                .registration()
                .bound()
                .is_none_or(|bound| bound.host_cancelled())
        {
            return Err(failure());
        }
        let source = self.current.upgrade().ok_or_else(failure)?;
        let services = source.runtime_services().map_err(|_| failure())?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|_| failure())?;
        for role in &self.roles {
            plan.role_slot(*role)
                .and_then(|slot| slot.service_admission())
                .ok_or_else(failure)?
                .check_current()?;
        }
        Ok(self.caller.clone())
    }

    fn resolve_packet_target(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        identity: zero_platform_tokio::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.binding
                || self.cancel.is_cancelled()
                || self.caller.is_joined()
                || self.egress.generation() != binding.network_generation.0
                || !matches!(&identity, zero_platform_tokio::contract::context::RequestIdentity::Service(facts) if matches!(facts.purpose, zero_core::request::ServicePurpose::DnsQuery | zero_core::request::ServicePurpose::Bootstrap))
            {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "Packet DNS target differs from its actual query source",
                ));
            }
            tokio::select! { biased;
                _ = self.cancel.cancelled() => Err(crate::component::instance::invalid(zero_platform_tokio::contract::Reason::Cancelled, "actual DNS caller stopped")),
                result = crate::protocol_registry::UpstreamConnectServices::resolve_packet_with(destination, identity, call, self.node_dns.as_ref()) => result,
            }
        })
    }

    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != &self.binding || self.cancel.is_cancelled() || self.caller.is_joined() {
            return None;
        }
        let source = self.current.upgrade()?;
        let services = source.runtime_services().ok()?;
        services
            .execution()
            .components
            .as_ref()
            .ok()?
            .pending_carrier_capacity(&self.roles)
            .ok()
    }

    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_pending(binding, adoption)?;
        self.pending_carrier_capacity(binding).ok_or_else(|| {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "DNS pending graph capacity is no longer available",
            )
        })?;
        Ok(())
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.retain_original_pending(binding, adoption)
    }

    fn supports_controlled_stream(&self) -> bool {
        self.direct_transport(&self.binding).is_ok()
    }
    fn carrier_dns_generation(&self) -> Option<zero_platform_tokio::contract::dns::DnsGeneration> {
        Some(self.node_dns.generation())
    }
    fn prepare_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> Result<zero_platform_tokio::controlled_carrier::PreparedCarrier, ChannelError> {
        if self.native.is_none() {
            return Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Unsupported,
                "DNS datagram observer cannot prepare a native Stream",
            ));
        }
        if endpoint != self.endpoint {
            return Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "DNS controlled root endpoint differs from its declaration",
            ));
        }
        let peer = match &endpoint.address {
            zero_core::Address::Ipv4(ip) => {
                SocketAddr::new(std::net::Ipv4Addr::from(*ip).into(), endpoint.port)
            }
            zero_core::Address::Ipv6(ip) => {
                SocketAddr::new(std::net::Ipv6Addr::from(*ip).into(), endpoint.port)
            }
            zero_core::Address::Domain(_) => {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Unsupported,
                    "DNS domain root requires its actual asynchronous Node resolver",
                ));
            }
        };
        self.retain_pending_carrier(binding, grant.adoption())?;
        NativeCarrierOpener::new(
            grant.instance(),
            binding.clone(),
            peer,
            self.egress.clone(),
            self.observer.clone(),
        )?
        .prepare_controlled_stream(binding, endpoint, call, grant)
    }
    fn resolve_controlled_carrier(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::CarrierCandidates, ChannelError>,
    > {
        self.resolve_native_datagram(binding.clone(), endpoint, call)
    }
    fn resolve_related_datagram(
        &self,
        binding: &UpstreamBinding,
        parent: zero_platform_tokio::controlled_carrier::CarrierAdoption,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::CarrierCandidates, ChannelError>,
    > {
        self.resolve_related_native_datagram(binding.clone(), parent, endpoint, call)
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        self.acquire_original(binding.clone(), endpoint, call, grant)
    }
    fn observe_carrier_origin(
        &self,
        origin: &zero_platform_tokio::controlled_carrier::CarrierOrigin,
    ) -> bool {
        let Ok(services) = self
            .current
            .upgrade()
            .ok_or(())
            .and_then(|source| source.runtime_services().map_err(|_| ()))
        else {
            return false;
        };
        let execution = services.execution();
        let Ok(plan) = execution.components.as_ref() else {
            return false;
        };
        let selected = self
            .roles
            .iter()
            .any(|role| plan.validate_carrier_origin(*role, origin).is_ok());
        if !selected {
            return false;
        }
        // Read-only observation of the already acquired same-slot original,
        // useful while protocol greeting/TLS still awaits. Cleanup authority
        // was independently retained from the actual grant before acquisition.
        zero_dns::OwnedDnsQueryTasks::record_native_for_caller(&self.caller, origin).is_ok()
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let end = call.deadline.ok_or_else(|| {
                crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    "DNS original requires its actual deadline",
                )
            })?;
            if self
                .original
                .lock()
                .expect("DNS original poisoned")
                .is_some()
            {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Unsupported,
                    "one DNS Stream cannot open a second original",
                ));
            }
            let candidates = self
                .native
                .as_ref()
                .ok_or_else(|| {
                    crate::component::instance::invalid(
                        zero_platform_tokio::contract::Reason::Unsupported,
                        "DNS datagram observer does not authorize native Stream acquisition",
                    )
                })?
                .resolve_controlled_carrier(&binding, endpoint, call)
                .await?;
            let mut last = None;
            for index in 0..candidates.len() {
                self.grants.reap_returned(end).await?;
                let capacity = self.budget.reserve(1).map_err(error)?;
                let mut grant = self.grants.reserve()?;
                let view = grant.claim_transferred_view()?;
                let entry = Arc::new(custody::NativeUse {
                    receiver: self.receiver.clone(),
                    pending_report: Mutex::default(),
                    protocol_originals: Mutex::default(),
                    task: self.caller.clone(),
                    cancel: self.cancel.clone(),
                    original: grant.adoption(),
                    pending_roles: Arc::from([]),
                    logical: Default::default(),
                    capacity: Mutex::new(Some(capacity)),
                });
                {
                    let mut users = self.users.lock().expect("DNS users poisoned");
                    if users.len() >= self.budget.limits().originals.get() {
                        return Err(crate::component::instance::invalid(
                            zero_platform_tokio::contract::Reason::Capacity,
                            "DNS original capacity exhausted",
                        ));
                    }
                    users.push(entry.clone());
                }
                let opened = async {
                    let io = candidates
                        .prepare_stream(index, grant)?
                        .open(end)
                        .await?
                        .into_parts()
                        .0;
                    let loan = io.into_stream_loan(view)?;
                    let origin = loan.owner.original().clone();
                    *entry.logical.lock().await = Some(loan.owner);
                    zero_dns::OwnedDnsQueryTasks::record_native(&origin).map_err(error)?;
                    *self.original.lock().expect("DNS original poisoned") = Some(entry.clone());
                    Ok(Box::pin(loan.io) as BoxStream)
                }
                .await;
                match opened {
                    Ok(io) => return Ok(io),
                    Err(opening) => {
                        entry.settle(end).await.map_err(error)?;
                        let retired = {
                            let mut users = self.users.lock().expect("DNS users poisoned");
                            users
                                .iter()
                                .position(|old| Arc::ptr_eq(old, &entry))
                                .map(|index| users.swap_remove(index))
                        };
                        drop(retired);
                        last = Some(opening);
                    }
                }
            }
            Err(last.unwrap_or_else(|| {
                crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Io,
                    "DNS Node candidates exhausted",
                )
            }))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: zero_platform_tokio::contract::datagram::DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        Box::pin(async {
            Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Unsupported,
                "DNS Stream cannot acquire Datagram",
            ))
        })
    }
}
