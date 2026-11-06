//! Real native closure precedes Host ACK; only the latter permits compaction.
use super::*;
#[tokio::test]
async fn actual_closed_owner_keeps_material_until_driver_registration_is_released() {
    timeout(Duration::from_secs(9), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let incoming = crate::grpc::accept_grpc_connection(
                io,
                &crate::profile::OwnedGrpcProfile {
                    service_names: vec!["owned".into()],
                    ..Default::default()
                },
            )
            .unwrap();
            let mut io = incoming.accept().await.unwrap();
            let mut byte = [0; 1];
            io.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, *b"r");
            io.write_all(&byte).await.unwrap();
            io.flush().await.unwrap();
            finished.await.unwrap();
            incoming.close();
        });
        let mut fixture = HostedFixture::new(peer, Arc::new(FailActualRead::default()));
        fixture.owner.lifecycle.start(deadline()).await.unwrap();
        let mut io = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        io.write_all(b"r").await.unwrap();
        io.flush().await.unwrap();
        let mut byte = [0; 1];
        io.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, *b"r");
        let managed = fixture.pool.0.managed(fixture.actual).unwrap();
        let weak = Arc::downgrade(&managed);
        let record = managed.records.lock().unwrap()[0].clone();
        let driver = record.witness.lock().unwrap().clone().unwrap();
        drop(managed);
        assert!(!fixture.pool.compact_closed_owner());
        drop(io);
        fixture
            .owner
            .lifecycle
            .revoke(zero_platform_tokio::contract::CloseReason::Shutdown);
        let receipt = fixture.scope.next_receipt().await.unwrap();
        assert_eq!(receipt.id(), driver.task_id());
        assert!(!driver.try_joined().unwrap().registration_released());
        let report = fixture.owner.lifecycle.shutdown(deadline()).await.unwrap();
        assert!(
            !report.is_complete(),
            "actual driver joined/native closed do not release Host ACK debt"
        );
        assert!(record.adoption.closed().unwrap().matches(&record.adoption));
        assert!(record
            .adoption
            .users_closed()
            .unwrap()
            .matches(&record.adoption));
        assert!(!fixture.pool.compact_closed_owner());
        assert!(weak.upgrade().is_some());
        fixture.scope.acknowledge(&receipt).unwrap();
        let joined = driver.wait_registration_released().await;
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        let report = fixture.owner.lifecycle.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert!(record.closed());
        assert!(fixture.pool.compact_closed_owner());
        // This fixed-library owner never acquires an ordinary legacy handoff.
        assert!(fixture.pool.validate_mode_candidate(false, true).is_err());
        drop(fixture.owner);
        assert!(
            weak.upgrade().is_none(),
            "memory assertion only after the above matching receipts"
        );
        fixture.scope.begin_shutdown();
        assert!(fixture.scope.next_receipt().await.is_none());
        assert!(fixture.scope.is_drained());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
