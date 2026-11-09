use super::*;
#[tokio::test]
async fn n1_sequential_rejections_stay_paid_backpressure_before_accept_and_preserve_every_original()
{
    timeout(Duration::from_secs(5), async {
        let root = packet_root(metadata_limit(&[1, 2, 4]));
        let view = root.driver_metadata_view();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
        let faults = AcceptedTaskFailures::prepare(instance(), &scope.registrar(), &view).unwrap();
        let mut owner = listener(&scope);
        // A real global original Root cannot silently select unpaid history.
        let originals = CarrierOriginalTotals::new(NonZeroUsize::new(2).unwrap());
        let denied = owner
            .bind_original_totals(instance(), &scope.registrar(), &originals)
            .unwrap_err();
        assert_eq!(denied.delivery, DeliveryState::NotSubmitted);
        owner.bind_task_failure_receiver(&faults).unwrap();
        owner
            .bind_original_totals(instance(), &scope.registrar(), &originals)
            .unwrap();
        let receiver = owner.accepted_task_receiver().unwrap();
        assert!(receiver.task_failures().unwrap().same_receiver(&faults));
        let mut io = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut originals_received = Vec::new();
        for _ in 0..4 {
            let actual = reject(&mut scope, &mut owner, &mut io).await;
            originals_received.push(actual);
            let report = owner.take_accepted_report();
            assert!(
                report.failed_tasks.is_empty(),
                "no unbounded shadow report Vec"
            );
            assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
            assert_eq!(
                originals.snapshot().charged,
                1,
                "only real listener remains"
            );
        }
        assert_eq!(faults.len(), 4);
        let before = view.snapshot();
        let next = rejected(io.accept_owned().await);
        assert_eq!(
            (next.reason, next.delivery),
            (Reason::Capacity, DeliveryState::NotSubmitted)
        );
        assert_eq!(owner.accepted_evidence().unwrap().retained, 0);
        assert_eq!(originals.snapshot().charged, 1);
        assert_eq!(view.snapshot(), before);
        // This is the actual ordinary Source admission wait, not a retry loop
        // spinning on the still-ready Carrier N book. No native is acquired.
        pending(io.accept_owned_wait()).await;
        assert_eq!(view.snapshot(), before);
        for (retained, received) in faults.iter().zip(&originals_received) {
            assert_eq!(retained.id(), received.id());
            assert!(retained.matches_joined(received));
        }
        // Full metadata never creates a shutdown allocation dependency.
        drop(io);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
        assert_eq!(
            originals.snapshot().charged,
            0,
            "same original listener and Scope truly retired"
        );
        let cursor = faults.iter();
        let delivery = faults.iter().next().unwrap();
        drop(originals_received); // no old raw Scope result escapes this actual receiver
        drop(receiver);
        drop(owner);
        drop(faults);
        assert_eq!(view.snapshot().occupied, before.occupied);
        drop(cursor);
        assert_eq!(
            view.snapshot().occupied,
            before.occupied,
            "sealed delivery still owns same Book"
        );
        assert!(delivery.result().is_err());
        drop(delivery);
        assert_eq!(
            view.snapshot().occupied,
            zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
        );
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn n1_successful_callers_reuse_the_actual_prepaid_fault_cell_without_new_allocations() {
    timeout(Duration::from_secs(5), async {
        let root = packet_root(metadata_limit(&[1]));
        let view = root.driver_metadata_view();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
        let faults = AcceptedTaskFailures::prepare(instance(), &scope.registrar(), &view).unwrap();
        let mut owner = listener(&scope);
        owner.bind_task_failure_receiver(&faults).unwrap();
        let mut io = owner.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let receiver = owner.accepted_task_receiver().unwrap();
        let original = view.snapshot();
        for _ in 0..12 {
            let (mut peer, accepted) = connection(&mut io).await;
            let claim = accepted.claim();
            let (id, _) = accepted
                .spawn_handler_bound(TaskClass::Auxiliary, |mut actual, _| async move {
                    actual.read_u8().await.map_err(|source| {
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                            .with_source(source)
                    })?;
                    Ok(())
                })
                .unwrap();
            peer.write_u8(1).await.unwrap();
            assert_eq!(scope.next_exit().await.unwrap().id, id);
            assert!(receiver.observe_joined(id).unwrap().result().is_ok());
            assert!(owner
                .close_accepted_graph(&claim, deadline())
                .await
                .unwrap()
                .unwrap()
                .matches(&claim));
            assert!(owner.take_accepted_report().failed_tasks.is_empty());
            assert_eq!(view.snapshot(), original);
            assert!(faults.is_empty());
        }
        drop(io);
        assert!(owner.wait_closed(deadline()).await.unwrap().is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn success_registration_and_observer_keep_old_header_ticket_until_actual_originals_drop() {
    let root = packet_root(metadata_limit(&[1]));
    let view = root.driver_metadata_view();
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), NonZeroUsize::MIN);
    let faults = AcceptedTaskFailures::prepare(instance(), &scope.registrar(), &view).unwrap();
    let mut reserved = faults.reserve().unwrap();
    reserved.begin_registration();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let registration = reserved.registered(&witness).unwrap();
    scope.next_exit().await.unwrap();
    let joined = witness.wait().await;
    registration.receive(&joined).unwrap();
    let observer = registration.clone();
    let occupied = view.snapshot();
    assert!(
        faults.reserve().is_err(),
        "live original header holders cannot reuse this ticket"
    );
    assert_eq!(view.snapshot(), occupied);
    drop(joined);
    drop(witness);
    drop(registration);
    assert!(
        faults.reserve().is_err(),
        "actual observer still pins original headers and their ticket"
    );
    assert_eq!(view.snapshot(), occupied);
    drop(observer);
    let reused = faults.reserve().unwrap();
    assert_eq!(
        view.snapshot(),
        occupied,
        "real header destruction permits the same prepaid promise"
    );
    drop(reused);
    assert!(scope.shutdown(deadline()).await.is_complete());
    drop(faults);
    assert_eq!(
        view.snapshot().occupied,
        zero_traits::packet_metadata::PacketMetadataCounter::owned_layout()
    );
}
