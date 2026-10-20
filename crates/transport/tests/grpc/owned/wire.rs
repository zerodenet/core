use super::*;
#[tokio::test]
async fn same_owned_pool_reuses_one_scoped_tls_free_driver_with_bounded_logical_workers_and_half_close(
) {
    timeout(Duration::from_secs(8), async {
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
            let mut workers = tokio::task::JoinSet::new();
            for _ in 0..3 {
                let mut stream = incoming.accept().await.unwrap();
                workers.spawn(async move {
                    let mut data = Vec::new();
                    stream.read_to_end(&mut data).await.unwrap();
                    stream.write_all(&data).await.unwrap();
                    stream.shutdown().await.unwrap();
                });
            }
            while let Some(result) = workers.join_next().await {
                result.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let mut fixture = Fixture::cold(peer, 1, 2, None, None);
        fixture.start().await;
        let managed = fixture.managed();
        let demand = fixture.demand();
        let first_cancel = demand.cancel.clone();
        let mut first = fixture.options.open(fixture.actual, demand).await.unwrap();
        first_cancel.cancel(); // The completed invocation never becomes the shared carrier's origin.
        let mut second = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        assert_eq!(
            fixture
                .options
                .open(fixture.actual, fixture.demand())
                .await
                .err()
                .unwrap()
                .reason,
            Reason::Capacity
        );
        assert_eq!(managed.records.lock().unwrap().len(), 1);
        let transfer = async |stream: &mut NativeGrpcStream, byte| {
            let payload = vec![byte; 196_613];
            stream.write_all(&payload).await.unwrap();
            stream.shutdown().await.unwrap();
            let mut echoed = Vec::new();
            stream.read_to_end(&mut echoed).await.unwrap();
            assert_eq!(echoed, payload);
        };
        tokio::join!(transfer(&mut first, 0x41), transfer(&mut second, 0x42));
        drop(first);
        drop(second);
        while managed.budget.retained() != 0 {
            tokio::task::yield_now().await;
        }
        let mut third = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        transfer(&mut third, 0x43).await;
        drop(third);
        assert_eq!(fixture.traffic.boundaries.load(Ordering::SeqCst), 1);
        assert!(fixture.traffic.tx.load(Ordering::SeqCst) > 3 * 196_613);
        assert!(fixture.traffic.rx.load(Ordering::SeqCst) > 3 * 196_613);
        let record = managed.records.lock().unwrap()[0].clone();
        let witness = record.witness.lock().unwrap().clone().unwrap();
        fixture
            .owner
            .lifecycle
            .revoke(zero_platform_tokio::contract::CloseReason::Shutdown);
        let joined = fixture.scope.next_exit().await.unwrap();
        assert_eq!(joined.id, witness.wait().await.id());
        assert!(joined.result.is_ok());
        assert!(
            record.adoption.closed().is_none(),
            "actual join alone is not physical closure"
        );
        assert_eq!(
            managed.grants.reserve().err().unwrap().reason,
            Reason::Revoked
        );
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert_eq!(protocol.closed_io.len(), 1);
        assert!(protocol.exited_tasks.is_empty());
        assert!(tasks.is_complete());
        assert!(
            tasks.exited_tasks.is_empty(),
            "the sole driver exit was already consumed"
        );
        assert!(record.closed());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn retired_carrier_limit_is_reusable_only_after_real_join_and_same_slot_native_close() {
    timeout(Duration::from_secs(8), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut owners = Vec::new();
            for _ in 0..2 {
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
                let mut body = [0; 4];
                io.read_exact(&mut body).await.unwrap();
                io.write_all(&body).await.unwrap();
                io.flush().await.unwrap();
                owners.push((incoming, io));
            }
            finished.await.unwrap();
            for (incoming, _io) in owners {
                incoming.close();
            }
        });
        let mut fixture = Fixture::cold(peer, 1, 2, None, None);
        fixture.start().await;
        let managed = fixture.managed();
        let mut first = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        first.write_all(b"live").await.unwrap();
        first.flush().await.unwrap();
        let mut bytes = [0; 4];
        first.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"live");
        let record = managed.records.lock().unwrap()[0].clone();
        let witness = record.witness.lock().unwrap().clone().unwrap();
        first.life.stop.cancel();
        drop(first);
        while !record.done.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(
            !witness.is_joined(),
            "body completion is not the actual scope join"
        );
        assert!(record.adoption.closed().is_none());
        assert_eq!(
            fixture
                .options
                .open(fixture.actual, fixture.demand())
                .await
                .err()
                .unwrap()
                .reason,
            Reason::Capacity
        );
        assert_eq!(managed.records.lock().unwrap().len(), 1);
        let exit = fixture.scope.next_exit().await.unwrap();
        assert!(exit.result.is_ok());
        assert!(witness.is_joined());
        assert!(record.adoption.closed().is_none());
        assert_eq!(
            managed.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        managed.reap(deadline()).await.unwrap();
        assert!(record.closed());
        assert!(managed.records.lock().unwrap().is_empty());
        assert_eq!(managed.budget.retained(), 0);
        let mut second = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        second.write_all(b"next").await.unwrap();
        second.flush().await.unwrap();
        second.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"next");
        let next = managed.records.lock().unwrap()[0].clone();
        assert!(!record.adoption.closed().unwrap().matches(&next.adoption));
        assert_eq!(fixture.traffic.boundaries.load(Ordering::SeqCst), 2);
        drop(second);
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert_eq!(protocol.closed_io.len(), 1);
        assert_eq!(tasks.exited_tasks.len(), 1);
        assert!(tasks.is_complete());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
