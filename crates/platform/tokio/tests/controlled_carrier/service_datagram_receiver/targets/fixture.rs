use super::*;
use crate::contract::{
    dns::*,
    lifecycle::DynChannelControl,
    scope::{HostedScope, TaskJoinWitness},
};
use crate::controlled_carrier::loan::{HostNodeDnsSource, ServiceDatagramTargetIssuer};
use std::sync::atomic::{AtomicUsize, Ordering};
pub(super) struct Node {
    pub calls: AtomicUsize,
    pub peer: std::net::SocketAddr,
}
impl DnsService for Node {
    fn generation(&self) -> DnsGeneration {
        DnsGeneration::new(4, 0)
    }
    fn resolve(
        &self,
        request: DnsRequest,
        context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>> {
        Box::pin(async move {
            assert_eq!(request.role, DnsQueryRole::Node);
            assert_eq!(request.name, "actual-quic.invalid");
            assert_eq!(context.expected_generation, Some(self.generation()));
            assert!(context.deadline.is_some() && !context.cancel.is_cancelled());
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(DnsResolution {
                addresses: vec![match self.peer.ip() {
                    std::net::IpAddr::V4(ip) => zero_traits::IpAddress::V4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_traits::IpAddress::V6(ip.octets()),
                }],
                generation: self.generation(),
            })
        })
    }
}
// This original has no protocol IO yet. The Root still independently waits
// its real auxiliary and native/user/Host receipts; this report supplies none.
struct Inert(CancellationToken);
impl DynChannelControl for Inert {
    fn cancel(&self, _: crate::contract::CloseReason) {
        self.0.cancel();
    }
    fn try_close(&mut self) -> Result<crate::contract::lifecycle::CloseReport, ChannelError> {
        self.0.cancel();
        Ok(Default::default())
    }
    fn wait_closed(
        &mut self,
        _: Instant,
    ) -> BoxFuture<'_, Result<crate::contract::lifecycle::CloseReport, ChannelError>> {
        self.0.cancel();
        Box::pin(async { Ok(Default::default()) })
    }
}
pub(super) struct Actual {
    pub scope: HostedScope,
    pub owner: CarrierOwner,
    pub grant: Option<CarrierGrant>,
    pub view: Option<crate::controlled_carrier::CarrierUserView>,
    pub original: crate::controlled_carrier::CarrierAdoption,
    pub actor: TaskJoinWitness,
    pub phase: crate::controlled_carrier::CarrierProtocolRetirement,
    pub host: Arc<CancellationToken>,
    pub bound: crate::controlled_carrier::CarrierOwnerBinding,
    pub binding: UpstreamBinding,
    pub endpoint: Destination,
    pub identity: crate::contract::context::RequestIdentity,
    pub call: CarrierContext,
    pub totals: Arc<CarrierOriginalTotals>,
    pub source: Arc<Node>,
}
impl Actual {
    pub async fn new(peer: std::net::SocketAddr, domain: bool) -> Self {
        let one = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
        scope.enable_registration().unwrap();
        let host = Arc::new(CancellationToken::new());
        let totals = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        let endpoint = if domain {
            Destination::new(Address::Domain("actual-quic.invalid".into()), peer.port())
        } else {
            destination(peer)
        };
        let binding = UpstreamBinding {
            prefix: prefix(),
            dial: Default::default(),
            network_generation: NetworkGeneration(0),
        };
        let mut owner = CarrierOwner::new(instance(), one, &CancellationToken::new());
        let bound = owner
            .registration()
            .bind_declared_with_totals(
                &ResourceDeclaration::ControlledServiceDatagram {
                    endpoint: endpoint.clone(),
                    dial: binding.dial.clone(),
                    max_sockets: one,
                },
                &scope.registrar(),
                &host,
                &totals,
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let stop = CancellationToken::new();
        let (mut grant, phase) = owner
            .grants()
            .reserve_protocol_original(&scope.registrar(), Box::new(Inert(stop.clone())).into())
            .unwrap();
        let original = grant.adoption();
        let view = grant.claim_transferred_view().unwrap();
        let (_, actor) = original
            .spawn_auxiliary(&scope.registrar(), async move {
                stop.cancelled().await;
                Ok(())
            })
            .unwrap();
        Self {
            scope,
            owner,
            grant: Some(grant),
            view: Some(view),
            original,
            actor,
            phase,
            host,
            bound,
            binding,
            endpoint,
            identity: crate::contract::context::RequestIdentity::Service(
                Arc::new(zero_core::request::ServiceRequestFacts {
                    origin: zero_core::request::ServiceOrigin::Runtime,
                    purpose: zero_core::request::ServicePurpose::BackgroundControl,
                })
                .into(),
            ),
            call: context(),
            totals,
            source: Arc::new(Node {
                calls: AtomicUsize::new(0),
                peer,
            }),
        }
    }
    pub fn issuer(&self) -> Result<ServiceDatagramTargetIssuer, ChannelError> {
        self.bound.service_datagram_target_issuer(
            &self.host,
            self.grant.as_ref().unwrap(),
            &self.actor,
            &self.binding,
            &self.endpoint,
            &self.identity,
            &self.call,
        )
    }
    pub fn node(&self) -> HostNodeDnsSource {
        HostNodeDnsSource::for_host(self.source.clone(), &self.host)
    }
    pub async fn finish(mut self) {
        drop(self.grant.take());
        drop(self.view.take());
        self.phase.request_retirement();
        let (native, host) = tokio::join!(
            self.original.retire_native(deadline()),
            self.scope.next_receipt()
        );
        assert!(native.unwrap().report().is_complete());
        let host = host.unwrap();
        assert!(self.actor.matches_joined(host.joined()));
        assert!(host.result().is_ok() && !host.joined().registration_released());
        assert!(self.original.closed().unwrap().matches(&self.original));
        assert!(self
            .original
            .users_closed()
            .unwrap()
            .matches(&self.original));
        assert!(
            self.totals.snapshot().charged > 0,
            "native/users do not repay the real actor Host ACK"
        );
        self.scope.acknowledge(&host).unwrap();
        self.actor.wait_registration_released().await;
        assert!(self
            .original
            .retire(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(self.owner.shutdown(deadline()).await.unwrap().is_complete());
        assert_eq!(self.totals.snapshot().charged, 0);
        self.scope.begin_shutdown();
        assert!(self.scope.is_drained());
    }
}
