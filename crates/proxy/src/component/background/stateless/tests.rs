//! Library custody contract over real I/O; production Engine admission is tested
//! separately through the ordinary SS/SOCKS business fixtures.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    component::{ComponentId, ComponentInstanceId, ResourceGeneration, SessionId},
    request::RequestFacts,
    Address,
};
use zero_platform_tokio::{
    contract::{
        context::RequestIdentity,
        scope::{TaskJoinWitness, TaskScope},
        stream::PeerConfirmation,
    },
    controlled_carrier::{CarrierBusinessDemand, CarrierOwnerBinding, NativeCarrierOpener},
};
mod acknowledgement;
mod preparation;
fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
struct Demand(CancellationToken);
impl CarrierBusinessDemand for Demand {
    fn check(&self) -> Result<(), ChannelError> {
        if self.0.is_cancelled() {
            Err(crate::component::instance::invalid(
                Reason::Revoked,
                "actual library demand revoked",
            ))
        } else {
            Ok(())
        }
    }
    fn session(&self) -> SessionId {
        SessionId(941)
    }
    fn cancellation(&self) -> CancellationToken {
        self.0.clone()
    }
    fn deadline(&self) -> Option<Instant> {
        None
    }
}
struct RawRole(Destination);
impl StreamOutbound for RawRole {
    type Stream = BoxStream;
    async fn connect(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        if request.destination.port == 0 {
            return Err(crate::component::instance::invalid(
                Reason::InvalidConfig,
                "protocol rejected before carrier I/O",
            ));
        }
        let io = context
            .upstream
            .open_stream(
                self.0.clone(),
                CarrierContext {
                    session: context.session_identity()?,
                    deadline: context.deadline,
                    cancel: context.cancel,
                },
            )
            .await?;
        Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        })
    }
}
struct Root {
    bound: CarrierOwnerBinding,
    host: Arc<CancellationToken>,
    peer: std::net::SocketAddr,
    binding: UpstreamBinding,
    egress: Arc<zero_platform_tokio::EgressInterfaceControl>,
    caller: TaskJoinWitness,
    demand: Arc<Demand>,
    facts: Arc<RequestFacts>,
}
impl UpstreamOpener for Root {
    fn has_tracked_business_caller(&self) -> bool {
        true
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        if binding != &self.binding
            || !matches!(identity, RequestIdentity::Business(facts) if Arc::ptr_eq(facts, &self.facts))
            || call.session != Some(SessionId(941))
            || self.caller.is_joined()
        {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "original library caller changed",
            ));
        }
        self.demand.check()?;
        Ok(self.caller.clone())
    }
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::controlled_carrier::CarrierIo, ChannelError>>
    {
        let binding = binding.clone();
        Box::pin(async move {
            let native = if call.session.is_some() {
                let ticket = self
                    .bound
                    .node_business_issuer(&self.host, &grant, &endpoint, &binding)?
                    .authorize_with_caller(self.demand.clone(), self.caller.clone())?;
                NativeCarrierOpener::business(ticket, self.egress.clone(), None)?
            } else {
                NativeCarrierOpener::new(
                    grant.instance(),
                    binding.clone(),
                    self.peer,
                    self.egress.clone(),
                    None,
                )?
            };
            Ok(native
                .prepare_controlled_stream(&binding, endpoint, call.clone(), grant)?
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { panic!("the actual original must use its controlled grant") })
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
        Box::pin(async { panic!("no unrelated Datagram original") })
    }
}
fn facts(endpoint: &Destination) -> Arc<RequestFacts> {
    Arc::new(RequestFacts {
        session: SessionId(941),
        inbound: zero_core::component::RoleId::Outbound(zero_core::component::OutboundId(941)),
        original_source: None,
        effective_source: None,
        local_endpoint: None,
        original_target: Some(endpoint.clone()),
        target: Some(endpoint.clone()),
        target_source: None,
        subject: None,
        peer: None,
        protocol: zero_core::ProtocolType::new("actual.library"),
        sni: None,
        process: None,
    })
}
