use super::*;
use futures_util::FutureExt;
#[derive(Debug)]
struct PanicAfterSocket;
impl zero_traits::IoObserver for PanicAfterSocket {
    fn stream_boundary(&self) {
        panic!("real socket acquisition outcome cannot be observed")
    }
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn real_native_acquisition_panic_keeps_same_slot_unknown_and_never_refunds_carrier_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut fixture = Fixture::cold(
        listener.local_addr().unwrap(),
        1,
        2,
        None,
        Some(Arc::new(PanicAfterSocket)),
    );
    fixture.start().await;
    let managed = fixture.managed();
    let result =
        std::panic::AssertUnwindSafe(fixture.options.open(fixture.actual, fixture.demand()))
            .catch_unwind()
            .await;
    assert!(result.is_err());
    let record = managed.records.lock().unwrap()[0].clone();
    assert!(record.witness.lock().unwrap().is_none());
    assert!(record.adoption.closed().is_none());
    let first = record.adoption.retire(deadline()).await.err().unwrap();
    let original = std::error::Error::source(&first)
        .unwrap()
        .downcast_ref::<Arc<ChannelError>>()
        .unwrap()
        .clone();
    assert_eq!(
        first.delivery,
        zero_platform_tokio::contract::DeliveryState::Unknown
    );
    for _ in 0..2 {
        let failure = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .err()
            .unwrap();
        // Reaping re-observes the actual unknown acquisition; it cannot open a second fd.
        assert_eq!(
            failure.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
        let source = std::error::Error::source(&failure)
            .unwrap()
            .downcast_ref::<Arc<ChannelError>>()
            .unwrap();
        assert!(Arc::ptr_eq(&original, source));
        assert!(record.adoption.closed().is_none());
        assert_eq!(managed.records.lock().unwrap().len(), 1);
        assert!(record.witness.lock().unwrap().is_none());
        assert_eq!(
            managed.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
    }
    let end = deadline();
    let (protocol, tasks) = tokio::join!(
        fixture.owner.lifecycle.shutdown(end),
        fixture.scope.shutdown(end)
    );
    assert_eq!(
        protocol.err().unwrap().delivery,
        zero_platform_tokio::contract::DeliveryState::Unknown
    );
    assert!(tasks.is_complete());
    assert!(tasks.exited_tasks.is_empty());
    assert!(record.adoption.closed().is_none());
}
#[derive(Debug, Default)]
struct PanicOnRead(std::sync::atomic::AtomicBool);
impl zero_traits::IoObserver for PanicOnRead {
    fn stream_boundary(&self) {}
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "original physical read observer failure"
        );
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn returned_stream_preserves_the_same_actual_scoped_driver_failure_arc() {
    timeout(Duration::from_secs(6), async {
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
            for expected in [b"live", b"fail"] {
                let mut body = [0; 4];
                io.read_exact(&mut body).await.unwrap();
                assert_eq!(&body, expected);
                io.write_all(&body).await.unwrap();
                io.flush().await.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let observer = Arc::new(PanicOnRead::default());
        let mut fixture = Fixture::cold(peer, 1, 2, None, Some(observer.clone()));
        fixture.start().await;
        let managed = fixture.managed();
        let mut io = fixture
            .options
            .open(fixture.actual, fixture.demand())
            .await
            .unwrap();
        io.write_all(b"live").await.unwrap();
        io.flush().await.unwrap();
        let mut bytes = [0; 4];
        io.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"live");
        let record = managed.records.lock().unwrap()[0].clone();
        let witness = record.witness.lock().unwrap().clone().unwrap();
        observer.0.store(true, Ordering::SeqCst);
        io.write_all(b"fail").await.unwrap();
        // The real native read callback panics in the one registered physical driver.
        let exit = fixture.scope.next_exit().await.unwrap();
        assert!(exit.result.is_err());
        assert_eq!(
            exit.class,
            zero_platform_tokio::contract::scope::TaskClass::Auxiliary
        );
        assert!(
            managed.ready(),
            "one recoverable carrier failure does not revoke the shared pool"
        );
        let original = witness.wait().await.result().unwrap_err();
        let failure = io.read_exact(&mut bytes).await.unwrap_err();
        let forwarded = failure
            .get_ref()
            .unwrap()
            .downcast_ref::<super::super::stream::SharedFailure>()
            .unwrap();
        assert!(Arc::ptr_eq(&forwarded.0, &original));
        assert!(std::error::Error::source(forwarded).is_some());
        assert!(record.adoption.closed().is_none());
        assert_eq!(
            managed.grants.reserve().err().unwrap().reason,
            Reason::Capacity
        );
        drop(io);
        let (protocol, tasks) = fixture.close().await;
        assert!(protocol.is_complete(), "{protocol:?}");
        assert!(tasks.exited_tasks.is_empty());
        assert!(record.closed());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
