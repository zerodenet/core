//! A per-demand Direct provider using the same canonical carrier/Scope. It
//! owns no socket collection, task scope, Engine or configuration snapshot.
mod io;
mod refresh;
use super::super::{business_carrier::BusinessCarrierRequest, instance::invalid};
use crate::{
    protocol_registry::UpstreamConnectServices,
    transport::{
        CandidateOpenFailure, CandidateOpened, CandidateOpener, CandidateRefresh, DirectCandidate,
        RefreshFuture, RefreshResult,
    },
};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use zero_core::{component::NetworkGeneration, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::WeakTaskRegistrar,
        BoxFuture, ChannelError, Reason,
    },
    controlled_carrier::{
        original_connect_error, CarrierAdoption, CarrierGrants, CarrierUserView,
        NativeCarrierOpener,
    },
    EgressInterfaceControl, EgressSelection,
};
use zero_traits::DialPolicy;

/// All entries originate from the actual built owner of the selected Slot.
/// Only the sealed original request can mint native business tickets.
#[derive(Clone)]
pub(in crate::component) struct ControlledDemand {
    pub(in crate::component) request: BusinessCarrierRequest,
    pub(in crate::component) grants: CarrierGrants,
    pub(in crate::component) tasks: WeakTaskRegistrar,
    pub(in crate::component) retirement: Duration,
}
pub(super) struct ControlledCandidates {
    demand: ControlledDemand,
    context: CarrierContext,
    services: UpstreamConnectServices,
    policy: DialPolicy,
    generation: NetworkGeneration,
    last_failed: Mutex<Option<CarrierAdoption>>,
}
impl ControlledCandidates {
    pub(super) fn new(
        demand: ControlledDemand,
        context: CarrierContext,
        services: UpstreamConnectServices,
        binding: UpstreamBinding,
    ) -> Result<Self, ChannelError> {
        if demand.retirement.is_zero() || context.session.is_none() {
            return Err(invalid(
                Reason::Unsupported,
                "controlled Direct requires an actual business demand and finite retirement budget",
            ));
        }
        Ok(Self {
            demand,
            context,
            services,
            policy: binding.dial,
            generation: binding.network_generation,
            last_failed: Mutex::new(None),
        })
    }
    fn binding(&self, generation: NetworkGeneration) -> UpstreamBinding {
        UpstreamBinding {
            prefix: vec![],
            dial: self.policy.clone(),
            network_generation: generation,
        }
    }
    fn destination(remote: SocketAddr) -> Destination {
        Destination::new(
            match remote.ip() {
                std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
            },
            remote.port(),
        )
    }
    fn keep_failure(&self, adoption: CarrierAdoption) {
        let old = self
            .last_failed
            .lock()
            .expect("Direct original failed slot")
            .replace(adoption);
        drop(old);
    }
    fn fail(&self, error: ChannelError) -> CandidateOpenFailure {
        let original = original_connect_error(&error);
        let (operation, interface_bound, local, kind, os_error) = match original {
            Some(source) => (
                source.stage(),
                source.interface_bound(),
                source.local_addr(),
                source.error().kind(),
                source.error().raw_os_error(),
            ),
            None => (
                "prepare_component",
                false,
                None,
                match error.reason {
                    Reason::Timeout => std::io::ErrorKind::TimedOut,
                    Reason::Cancelled | Reason::Revoked => std::io::ErrorKind::Interrupted,
                    Reason::Unsupported => std::io::ErrorKind::Unsupported,
                    _ => std::io::ErrorKind::Other,
                },
                None,
            ),
        };
        CandidateOpenFailure {
            operation,
            interface_bound,
            local,
            os_error,
            source: std::io::Error::new(kind, OriginalChannel(Arc::new(error))),
        }
    }
    async fn open_candidate(
        &self,
        candidate: DirectCandidate,
        selection: &EgressSelection,
        policy: &DialPolicy,
    ) -> Result<CandidateOpened<io::DirectIo>, ChannelError> {
        let remote = candidate.peer();
        if candidate.generation() != self.generation
            || candidate.generation().0 != selection.generation()
            || policy != &self.policy
            || selection.generation() != self.services.network_generation()
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct candidate policy/generation changed",
            ));
        }
        let retirement_end = Instant::now()
            .checked_add(self.demand.retirement)
            .ok_or_else(|| {
                invalid(
                    Reason::InvalidConfig,
                    "Direct retirement budget exceeds monotonic time range",
                )
            })?;
        tracing::trace!(
            ?remote,
            "Direct controlled candidate reaping original slots"
        );
        self.demand.grants.reap_returned(retirement_end).await?;
        tracing::trace!(
            ?remote,
            "Direct controlled candidate reserving original slot"
        );
        let mut grant = self.demand.grants.reserve()?;
        let user = grant.claim_user_view()?;
        let adoption = grant.adoption();
        let endpoint = Self::destination(remote);
        let binding = self.binding(NetworkGeneration(selection.generation()));
        let result = async {
            let ticket = self
                .demand
                .request
                .authorize_direct(&candidate, &binding, &grant)?;
            // Direct flow traffic keeps its original relay accounting. Do not
            // introduce proxy outer-TLS/handshake bytes or an extra observer.
            let native = NativeCarrierOpener::business(
                ticket,
                Arc::new(self.services.egress_control()),
                None,
            )?;
            tracing::trace!(
                ?remote,
                "Direct controlled candidate starting native connect"
            );
            let carrier = native
                .prepare_controlled_stream(&binding, endpoint, self.context.clone(), grant)?
                .open_with_deadline(self.context.deadline)
                .await?;
            let local = carrier
                .endpoints()
                .local()
                .ok_or_else(|| invalid(Reason::Internal, "missing actual Direct local endpoint"))?;
            let actual_peer = carrier.endpoints().peer();
            if actual_peer != remote
                || !carrier.endpoints().matches(&adoption)
                || !user.matches(&adoption)
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "Direct native evidence differs from original candidate",
                ));
            }
            let (io, _) = carrier.into_parts();
            Ok(CandidateOpened {
                io: io::DirectIo::new(io, user),
                remote: actual_peer,
                local,
            })
        }
        .await;
        if result.is_err() {
            self.keep_failure(adoption);
        }
        result
    }
}
impl CandidateOpener for ControlledCandidates {
    type Socket = io::DirectIo;
    fn open<'a>(
        &'a self,
        candidate: DirectCandidate,
        _: &'a EgressInterfaceControl,
        selection: &'a EgressSelection,
        policy: &'a DialPolicy,
    ) -> BoxFuture<'a, Result<CandidateOpened<Self::Socket>, CandidateOpenFailure>> {
        Box::pin(async move {
            self.open_candidate(candidate, selection, policy)
                .await
                .map_err(|error| self.fail(error))
        })
    }
    fn refresh(
        &self,
        task: RefreshFuture,
        source: CandidateRefresh,
    ) -> BoxFuture<'_, RefreshResult> {
        Box::pin(async move { Ok(self.refresh_owned(task, source).await) })
    }
}
#[derive(Debug)]
struct OriginalChannel(Arc<ChannelError>);
impl std::fmt::Display for OriginalChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for OriginalChannel {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn original_channel(error: &std::io::Error) -> Option<Arc<ChannelError>> {
    error
        .get_ref()?
        .downcast_ref::<OriginalChannel>()
        .map(|source| source.0.clone())
}
