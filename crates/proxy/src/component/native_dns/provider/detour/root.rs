//! A standard Stream role receives I/O owned by this actual DNS host.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{lifecycle::DynChannelControl, stream::BoxStream, ChannelError, CloseReason},
    controlled_carrier::{CarrierGrants, ResolvingCarrierOpener},
};
pub(super) struct Root {
    native: ResolvingCarrierOpener,
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
        current: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
        roles: Vec<zero_core::component::RoleInstanceId>,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            native: ResolvingCarrierOpener::new(
                grants.registration().instance(),
                binding.clone(),
                endpoint.clone(),
                services.native_node_dns(),
                Arc::new(services.egress_control()),
                services.observer(),
            )?,
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
        })
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
            || !self.caller.scope_identity().registration_open()
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
        Ok(StreamLoan::new(io, Box::new(Control(entry)), original))
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
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_pending(binding, &adoption)?;
        let mut pending = self.pending.lock().expect("DNS pending originals poisoned");
        // Candidate retries share one actual caller. Completed earlier native
        // attempts are not concurrent occupancy or a retained query history.
        // Unknown native disposition or users still held remain charged here.
        pending.retain(|entry| {
            !entry.task.same_registration(&self.caller)
                || !entry
                    .original
                    .closed()
                    .is_some_and(|closed| closed.matches(&entry.original))
                || !entry
                    .original
                    .users_closed()
                    .is_some_and(|closed| closed.matches(&entry.original))
        });
        if pending.iter().any(|entry| {
            entry.task.same_registration(&self.caller) && entry.original.same_slot(&adoption)
        }) {
            return Ok(());
        }
        // A selected path has finitely many roles and at most one in-progress
        // donor plus its actual receiver per hop. Actual query/driver scopes
        // independently bound the number of callers; completed history is absent.
        let ceiling = self.roles.len().checked_mul(2).ok_or_else(|| {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Capacity,
                "DNS pending path capacity overflow",
            )
        })?;
        if pending
            .iter()
            .filter(|entry| entry.task.same_registration(&self.caller))
            .count()
            >= ceiling
        {
            return Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Capacity,
                "DNS actual pending carrier path is full",
            ));
        }
        pending.push(Arc::new(custody::NativeUse {
            task: self.caller.clone(),
            cancel: self.cancel.clone(),
            original: adoption,
            capacity: Mutex::new(None),
            logical: Default::default(),
        }));
        Ok(())
    }
    fn supports_controlled_stream(&self) -> bool {
        true
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
                ))
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
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        let binding = binding.clone();
        Box::pin(async move {
            if endpoint != self.endpoint {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "DNS controlled root endpoint differs from its declaration",
                ));
            }
            self.retain_pending_carrier(&binding, grant.adoption())?;
            let opener = ResolvingCarrierOpener::new(
                grant.instance(),
                binding.clone(),
                endpoint.clone(),
                self.node_dns.clone(),
                self.egress.clone(),
                self.observer.clone(),
            )?;
            let candidates = opener
                .resolve_controlled_carrier(&binding, endpoint, call.clone())
                .await?;
            self.check_pending(&binding, &grant.adoption())?;
            let prepared = candidates.prepare_stream(0, grant)?;
            Ok(prepared
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0)
        })
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
        self.roles
            .iter()
            .any(|role| plan.validate_carrier_origin(*role, origin).is_ok())
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
                .resolve_controlled_carrier(&binding, endpoint, call)
                .await?;
            let mut last = None;
            for index in 0..candidates.len() {
                self.grants.reap_returned(end).await?;
                let capacity = self.budget.reserve(1).map_err(error)?;
                let mut grant = self.grants.reserve()?;
                let view = grant.claim_transferred_view()?;
                let entry = Arc::new(custody::NativeUse {
                    task: self.caller.clone(),
                    cancel: self.cancel.clone(),
                    original: grant.adoption(),
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
