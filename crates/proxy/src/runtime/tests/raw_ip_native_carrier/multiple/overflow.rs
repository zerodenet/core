//! Native close alone cannot discard an original whose users remain unknown.
use super::*;

#[tokio::test]
async fn closed_native_originals_without_user_evidence_stay_charged_at_the_actual_owner_limit() {
    let mut fixture = Fixture::new().await;
    let physical = fixture.carrier.registration();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let egress = Arc::new(EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        prefix: Vec::new(),
        dial: Default::default(),
        network_generation: NetworkGeneration(egress.generation()),
    };
    let opener = NativeCarrierOpener::new(
        fixture.carrier.instance(),
        binding.clone(),
        address,
        egress,
        None,
    )
    .unwrap();
    let mut originals = Vec::new();
    for _ in 0..physical.limit().get() {
        let grant = fixture.carrier.grants().reserve().unwrap();
        let original = grant.adoption();
        fixture
            .registration
            .observe_carrier_with_owner(&physical, original.clone())
            .unwrap();
        let prepared = opener
            .prepare_controlled_stream(
                &binding,
                Destination::new(Address::Ipv4([127, 0, 0, 1]), address.port()),
                CarrierContext {
                    session: None,
                    deadline: None,
                    cancel: CancellationToken::new(),
                },
                grant,
            )
            .unwrap();
        let carrier = prepared
            .open(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let (mut io, adoption) = carrier.into_parts();
        assert!(adoption.same_slot(&original));
        io.write_all(b"actual original").await.unwrap();
        let mut bytes = [0; 15];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"actual original");
        drop(io);
        adoption
            .retire(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
        assert!(adoption.closed().unwrap().had_native_acquisition());
        assert!(
            adoption.users_closed().is_none(),
            "untracked library user is not a business C user receipt"
        );
        originals.push(adoption);
    }
    // The actual native-only library owner can reserve after physical close,
    // but the raw observer cannot wash its original missing-user obligations.
    let next = fixture.carrier.grants().reserve().unwrap();
    assert_eq!(
        fixture
            .registration
            .observe_carrier_with_owner(&physical, next.adoption())
            .unwrap_err()
            .reason,
        Reason::Capacity
    );
    {
        let state = fixture.native.0.lock().unwrap();
        let record = state.records.get(&fixture.registration.id).unwrap();
        assert!(record.carrier_conflict);
        assert_eq!(
            record.additional_carriers.len() + usize::from(record.carrier.is_some()),
            physical.limit().get()
        );
        for original in originals {
            assert!(record
                .carrier
                .iter()
                .chain(&record.additional_carriers)
                .any(|actual| actual.same_slot(&original)));
        }
    }
    drop(next);
    fixture.join().await;
    fixture
        .carrier
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    assert!(fixture.registration.carrier_completion().is_none());
    fixture.assert_gate_retained().await;
}
