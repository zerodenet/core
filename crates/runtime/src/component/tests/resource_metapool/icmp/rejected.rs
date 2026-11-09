//! Real metadata IO without invoking a capability-dependent ICMP syscall.
use super::*;
#[tokio::test]
async fn actual_metadata_icmp_rejects_foreign_root_host_and_cancelled_original_without_acquiring_icmp(
) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let end = Instant::now() + std::time::Duration::from_secs(3);
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = peer.local_addr().unwrap();
        let fixture = Fixture::with_icmp(remote).await;
        let foreign = Fixture::with_icmp(remote).await;
        let originals = fixture.slot.bound_original_budgets().unwrap();
        let resources = fixture.resources.clone();
        let foreign_resources = foreign.resources.clone();
        let actual = fixture.slot.instance_id();
        let (begin, started) =
            tokio::sync::oneshot::channel::<(MetadataPoolPermit, TaskJoinWitness)>();
        let (ready, opened) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (_, query) = fixture
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let (permit, query) = started.await.unwrap();
                let memory = MetadataIcmpMemory::reserve(&resources.packet_driver_metadata()?)?;
                let foreign_memory =
                    MetadataIcmpMemory::reserve(&foreign_resources.packet_driver_metadata()?)?;
                let egress = Arc::new(zero_platform_tokio::EgressInterfaceControl::default());
                let binding = zero_platform_tokio::contract::network::UpstreamBinding {
                    prefix: vec![],
                    dial: Default::default(),
                    network_generation: NetworkGeneration(egress.generation()),
                };
                let endpoint = zero_core::request::Destination::new(
                    zero_core::Address::Ipv4([127, 0, 0, 1]),
                    remote.port(),
                );
                let cancel = zero_platform_tokio::contract::CancellationToken::new();
                use zero_platform_tokio::contract::network::UpstreamOpener;
                let candidates = zero_platform_tokio::controlled_carrier::NativeCarrierOpener::new(
                    actual,
                    binding.clone(),
                    remote,
                    egress,
                    None,
                )
                .unwrap()
                .resolve_controlled_carrier(
                    &binding,
                    endpoint,
                    zero_platform_tokio::contract::network::CarrierContext {
                        session: None,
                        deadline: Some(end),
                        cancel: cancel.clone(),
                    },
                )
                .await?;
                let (_, wire) = candidates.open_metadata(0, None, &permit, &query, end)?;
                assert!(candidates
                    .prepare_icmp(0, None, &permit, &query, foreign_memory)
                    .is_err());
                let attempt = candidates.prepare_icmp(0, None, &permit, &query, memory)?;
                let observed = attempt.observation();
                assert!(foreign_resources
                    .bind_outbound_icmp(attempt.clone())
                    .is_err());
                assert!(
                    matches!(observed.disposition(), IcmpAttemptDisposition::Pending),
                    "a foreign Host cannot poison the original attempt"
                );
                cancel.cancel();
                let failure = resources
                    .bind_outbound_icmp(attempt.clone())
                    .err()
                    .expect("actual original cancellation prevents ICMP acquisition");
                assert_eq!(
                    failure.reason,
                    zero_platform_tokio::contract::Reason::Cancelled
                );
                attempt.reject_unissued();
                assert!(matches!(
                    observed.disposition(),
                    IcmpAttemptDisposition::KnownNoNative
                ));
                ready
                    .send((permit, wire, observed))
                    .unwrap_or_else(|_| panic!("same actual receiver"));
                let _ = finished.await;
                Ok(())
            })
            .unwrap();
        let permit = fixture.grants.reserve(&query).unwrap();
        begin
            .send((permit, query.clone()))
            .unwrap_or_else(|_| panic!("actual original initializer"));
        let (permit, wire, observed) = fixture.drive(opened).await.unwrap();
        assert!(matches!(
            observed.disposition(),
            IcmpAttemptDisposition::KnownNoNative
        ));
        assert_eq!(
            originals.carrier.snapshot().charged,
            1,
            "no ICMP original was acquired"
        );
        let returned = wire
            .return_unstarted()
            .unwrap_or_else(|_| panic!("same original UDP views"));
        let users = returned.try_closed().unwrap();
        let native = permit.close(Some(&users), &fixture.tasks).unwrap();
        assert!(native.matches(&permit.origin()) && native.had_native_acquisition());
        assert_eq!(fixture.grants.retained(), Some(1));
        fixture.complete_query(&query, finish).await;
        assert!(fixture.finish().await.is_complete());
        assert!(foreign.finish().await.is_complete());
        assert_eq!(originals.carrier.snapshot().charged, 0);
    })
    .await
    .unwrap();
}
