use super::*;
use crate::contract::{
    component::{ControlledStreamDemandClass, ResourceDeclaration},
    dns::{DnsGeneration, DnsQueryContext, DnsQueryRole, DnsRequest, DnsResolution, DnsService},
    network::UpstreamOpener,
    BoxFuture, ChannelError,
};

struct Addresses(DnsGeneration);
impl DnsService for Addresses {
    fn generation(&self) -> DnsGeneration {
        self.0
    }
    fn resolve(
        &self,
        request: DnsRequest,
        context: DnsQueryContext,
    ) -> BoxFuture<'_, Result<DnsResolution, ChannelError>> {
        assert_eq!(request.role, DnsQueryRole::Node);
        assert_eq!(request.name, "owned.example");
        assert_eq!(context.expected_generation, Some(self.0));
        let generation = self.0;
        Box::pin(async move {
            Ok(DnsResolution {
                addresses: vec![
                    zero_traits::IpAddress::V4([255, 255, 255, 255]),
                    zero_traits::IpAddress::V6(std::net::Ipv6Addr::LOCALHOST.octets()),
                ],
                generation,
            })
        })
    }
}

#[tokio::test]
async fn resolved_candidate_uses_original_native_close_before_the_next_actual_connection() {
    timeout(Duration::from_secs(6), async {
        let listener = TcpListener::bind("[::1]:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let egress = Arc::new(crate::EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let endpoint = Destination::new(Address::Domain("owned.example".into()), peer.port());
        let resolver = Arc::new(Addresses(DnsGeneration::new(17, egress.generation())));
        let opener = ResolvingCarrierOpener::new(
            instance(),
            binding.clone(),
            endpoint.clone(),
            resolver,
            egress,
            None,
        )
        .unwrap();
        let mut owner = CarrierOwner::new(
            instance(),
            NonZeroUsize::new(1).unwrap(),
            &CancellationToken::new(),
        );
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let original = owner.registration();
        original
            .bind_declared(
                &ResourceDeclaration::ControlledOutboundStream {
                    endpoint: endpoint.clone(),
                    binding: binding.clone(),
                    demand: ControlledStreamDemandClass::BackgroundControl,
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &Arc::new(CancellationToken::new()),
            )
            .unwrap();
        owner.start(deadline()).await.unwrap();
        let candidates = opener
            .resolve_controlled_carrier(&binding, endpoint, context())
            .await
            .unwrap();
        assert_eq!(candidates.len(), 2);
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_driver_view().unwrap();
        let failed = grant.adoption();
        let prepared = candidates.prepare_stream(0, grant).unwrap();
        let observed = original.observe().unwrap();
        assert_eq!(observed.retained(), 1);
        assert!(
            observed.slots()[0]
                .native()
                .is_some_and(|native| !native.is_closed()),
            "actual original TCP control is installed before connect"
        );
        assert!(prepared.open(deadline()).await.is_err());
        assert!(
            failed.closed().is_none(),
            "a real connect error is not original close"
        );
        assert!(
            owner.grants().reserve().is_err(),
            "the original still charges the single slot"
        );
        drop(view);
        assert!(failed.retire(deadline()).await.unwrap().is_complete());
        let receipt = failed.closed().unwrap();
        assert!(receipt.matches(&failed) && receipt.had_native_acquisition());
        assert!(
            receipt.driver_exit().is_none(),
            "no fabricated driver was needed for failed preparation"
        );
        let mut grant = owner.grants().reserve().unwrap();
        let view = grant.claim_driver_view().unwrap();
        let connected = grant.adoption();
        let ready = candidates
            .prepare_stream(1, grant)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (_peer, _) = listener.accept().await.unwrap();
        let (io, _) = ready.into_parts();
        drop(io);
        drop(view);
        assert!(connected.retire(deadline()).await.unwrap().is_complete());
        assert!(connected.closed().unwrap().had_native_acquisition());
        assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
        assert!(original.closed().unwrap().matches(&original));
        assert!(original.users_closed().unwrap().matches(&original));
        let report = scope.shutdown(deadline()).await;
        assert!(report.is_complete() && report.exited_tasks.is_empty());
    })
    .await
    .unwrap();
}
