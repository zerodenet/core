use super::*;
#[tokio::test]
async fn a_received_actor_fault_cannot_hide_a_different_actual_late_actor_receipt() {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(3));
        scope.enable_registration().unwrap();
        let tasks = scope.registrar();
        let (mut wire, mut peer) = tokio::io::duplex(8);
        let (ready, pending) = tokio::sync::oneshot::channel();
        let (_, main) = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let mut byte = [0];
                let reading = wire.read_exact(&mut byte);
                tokio::pin!(reading);
                poll_fn(|cx| {
                    assert!(reading.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                ready.send(()).unwrap();
                reading
                    .await
                    .map_err(|e| failure(Reason::Io).with_source(e))?;
                Ok(())
            })
            .unwrap();
        pending.await.unwrap();
        let instance = ComponentInstanceId {
            component: ComponentId(491),
            generation: ResourceGeneration(1),
        };
        let software = LogicalPacketProviderRegistration::from_registered(
            PacketDeviceIdentity::new(instance, tasks.scope_identity()),
            main.clone(),
            tasks.cancellation(),
        )
        .unwrap();
        let total = CarrierOriginalTotals::new(NonZeroUsize::MIN);
        software
            .reserve_original_totals(&tasks.downgrade(), &total)
            .unwrap();
        let original = CarrierOrigin::from_logical_packet(software);
        let root = Arc::new(
            PacketDriverCalls::from_registered(original.clone(), &tasks.downgrade()).unwrap(),
        );
        #[cfg(feature = "native-packet-origin")]
        {
            let memory = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
            root.install_send_workspaces(
                &original,
                vec![(
                    main.clone(),
                    memory.native_grant(255).unwrap(),
                    memory.native_grant(255).unwrap(),
                )],
            )
            .unwrap();
        }
        let (first, mut first_peer) = io::pending_actor(&root, &tasks, &main).await;
        let (second, mut second_peer) = io::pending_actor(&root, &tasks, &main).await;
        first_peer.write_all(b"no").await.unwrap();
        let a = scope.next_receipt().await.unwrap();
        assert!(first.matches_joined(a.joined()));
        let receiver = crate::contract::lifecycle::ResourceCleanupFailures::default();
        let one = Arc::new(root.receive_actor_failures().unwrap());
        let first_receipt = one.cleanup_failure_receipt().unwrap().clone();
        assert!(receiver.try_receive(
            one.clone(),
            crate::contract::lifecycle::CleanupFailurePhase::Driver
        ));
        assert!(
            !second.is_joined(),
            "the real second actor still waits in its original IO"
        );
        second_peer.write_all(b"no").await.unwrap();
        let b = scope.next_receipt().await.unwrap();
        assert!(second.matches_joined(b.joined()));
        let two = Arc::new(root.receive_actor_failures().unwrap());
        assert!(
            first_receipt.same_receipt(two.cleanup_failure_receipt().unwrap()),
            "the actual Resource keeps one stable pure Source payload"
        );
        let actors = root.reconnects.lock().unwrap();
        let child_one = actors[0].fault_report().unwrap();
        let child_two = actors[1].fault_report().unwrap();
        assert!(!child_one
            .cleanup_failure_receipt()
            .unwrap()
            .same_receipt(child_two.cleanup_failure_receipt().unwrap()));
        assert!(root.source_failures.retains_child(&child_one));
        assert!(root.source_failures.retains_child(&child_two));
        assert!(child_one
            .cleanup_failure_receipt()
            .unwrap()
            .is_acknowledged());
        assert!(
            child_two
                .cleanup_failure_receipt()
                .unwrap()
                .is_acknowledged(),
            "new exact child report was persisted before its independent receipt ACK"
        );
        assert!(has_actual_source(&child_one, &a.result().unwrap_err()));
        assert!(has_actual_source(&child_two, &b.result().unwrap_err()));
        drop(actors);
        assert!(receiver.try_receive(
            two.clone(),
            crate::contract::lifecycle::CleanupFailurePhase::Driver
        ));
        assert!(
            !root.cleanup_settled(),
            "receiving errors does not release actual actor registrations"
        );
        assert_eq!(total.snapshot().charged, 1);
        scope.acknowledge(&a).unwrap();
        scope.acknowledge(&b).unwrap();
        first.wait_registration_released().await;
        second.wait_registration_released().await;
        let completed = root
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(completed.is_complete());
        let failure = completed.resource_failures.first().unwrap();
        assert!(Arc::ptr_eq(&failure.source, one.retained_source().unwrap()));
        assert!(has_actual_source(&failure.source, &a.result().unwrap_err()));
        // The same mutable pure payload also retains the distinct later actor.
        assert!(root.source_failures.retains_child(&child_two));
        assert_eq!(
            total.snapshot().charged,
            1,
            "original main and whole Source debt remain independent"
        );
        assert!(original.closed().is_none());
        scope.begin_shutdown();
        peer.write_all(&[1]).await.unwrap();
        let exit = scope.next_receipt().await.unwrap();
        assert!(main.matches_joined(exit.joined()));
        scope.acknowledge(&exit).unwrap();
        assert!(scope.is_drained());
        assert_eq!(total.snapshot().charged, 0);
        assert!(first_receipt.is_acknowledged());
        assert!(has_actual_source(&one, &a.result().unwrap_err()));
    })
    .await
    .unwrap();
}
