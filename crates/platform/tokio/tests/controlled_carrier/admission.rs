use super::*;
use crate::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    network::UpstreamOpener,
    stream::BoxStream,
    BoxFuture,
};
#[tokio::test]
async fn controlled_carrier_cold_finite_roleless_grant_and_default_deny_prevent_legacy_io() {
    struct Trap;
    impl UpstreamOpener for Trap {
        fn open_stream(
            &self,
            _: &UpstreamBinding,
            _: Destination,
            _: CarrierContext,
        ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
            panic!("new capability must never call the legacy opener")
        }
        fn open_datagram(
            &self,
            _: &UpstreamBinding,
            _: DatagramRequest,
            _: CarrierContext,
        ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
            panic!("no datagram fallback")
        }
    }
    let mut owner = CarrierOwner::new(
        instance(),
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    let grants = owner.grants();
    assert_eq!(grants.reserve().err().unwrap().reason, Reason::Revoked);
    owner.start(deadline()).await.unwrap();
    let first = grants.reserve().unwrap();
    assert_eq!(first.instance(), instance());
    assert_eq!(grants.reserve().err().unwrap().reason, Reason::Capacity);
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: Default::default(),
        network_generation: NetworkGeneration(3),
    };
    let transport = UpstreamTransport::new(binding, Arc::new(Trap));
    let failure = transport
        .prepare_controlled_stream(
            destination("127.0.0.1:9".parse().unwrap()),
            context(),
            first,
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::Unsupported);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(failure.component, Some(instance().component));
    assert_eq!(failure.session, None);
    assert_eq!(
        owner.retained(),
        0,
        "unattempted default rejection releases only its unused reservation"
    );
    drop(grants.reserve().unwrap());
    assert!(owner.shutdown(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn controlled_carrier_wrong_owner_destination_session_or_generation_rejects_before_native_prepare(
) {
    let mut fixture = Fixture::new(1).await;
    let mut context = context();
    context.session = Some(zero_core::component::SessionId(0));
    let failure = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            context,
            fixture.grants.reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
    assert_eq!(fixture.owner.retained(), 0);
    let failure = fixture
        .transport
        .prepare_controlled_stream(
            destination("127.0.0.1:1".parse().unwrap()),
            super::context(),
            fixture.grants.reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    let other = ComponentInstanceId {
        generation: ResourceGeneration(20),
        ..instance()
    };
    let mut other_owner = CarrierOwner::new(
        other,
        NonZeroUsize::new(1).unwrap(),
        &CancellationToken::new(),
    );
    other_owner.start(deadline()).await.unwrap();
    let failure = fixture
        .transport
        .prepare_controlled_stream(
            fixture.destination.clone(),
            super::context(),
            other_owner.grants().reserve().unwrap(),
        )
        .err()
        .unwrap();
    assert_eq!(failure.reason, Reason::StalePlan);
    assert_eq!(other_owner.retained(), 0);
    assert!(
        timeout(Duration::from_millis(20), fixture.listener.accept())
            .await
            .is_err()
    );
    assert!(fixture
        .owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(other_owner
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
}
#[tokio::test]
async fn controlled_carrier_network_generation_fences_keep_already_prepared_original_control() {
    timeout(Duration::from_secs(3), async {
        let mut fixture = Fixture::new(1).await;
        fixture.egress.invalidate_network();
        let failure = fixture
            .transport
            .prepare_controlled_stream(
                fixture.destination.clone(),
                context(),
                fixture.grants.reserve().unwrap(),
            )
            .err()
            .unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        assert_eq!(failure.delivery, DeliveryState::NotSubmitted);
        assert_eq!(fixture.owner.retained(), 0);
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());

        let mut fixture = Fixture::new(1).await;
        let prepared = fixture.prepare();
        fixture.egress.invalidate_network();
        let failure = prepared.open(deadline()).await.err().unwrap();
        assert_eq!(failure.reason, Reason::StalePlan);
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        assert_eq!(fixture.owner.retained(), 1);
        assert_eq!(
            fixture.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        assert!(
            timeout(Duration::from_millis(20), fixture.listener.accept())
                .await
                .is_err()
        );
        let report = fixture.owner.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(report.closed_io.len(), 1);
        assert_eq!(fixture.owner.retained(), 0);
    })
    .await
    .unwrap();
}
