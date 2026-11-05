//! A standard Stream role receives I/O owned by this actual DNS host.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{lifecycle::DynChannelControl, stream::BoxStream, ChannelError, CloseReason},
    controlled_carrier::{CarrierGrants, ResolvingCarrierOpener},
};
pub(super) struct Root {
    native: ResolvingCarrierOpener,
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
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        current: std::sync::Weak<crate::runtime::current_services::CurrentServiceSource>,
        roles: Vec<zero_core::component::RoleInstanceId>,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            native: ResolvingCarrierOpener::new(
                grants.registration().instance(),
                binding,
                endpoint,
                services.native_node_dns(),
                Arc::new(services.egress_control()),
                services.observer(),
            )?,
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
