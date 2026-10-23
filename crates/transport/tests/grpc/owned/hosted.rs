use super::*;
use std::sync::atomic::AtomicBool;
use zero_platform_tokio::contract::scope::{HostedScope, SpawnError, TaskClass};

#[derive(Debug, Default)]
struct FailActualRead(AtomicBool);
impl zero_traits::IoObserver for FailActualRead {
    fn stream_boundary(&self) {}
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "original physical read callback failed"
        );
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
struct HostedFixture {
    pool: GrpcPool,
    options: Arc<NativePoolOptions>,
    actual: ComponentInstanceId,
    binding: UpstreamBinding,
    scope: HostedScope,
    owner: ComponentInstance,
}
impl HostedFixture {
    fn new(peer: std::net::SocketAddr, observer: Arc<FailActualRead>) -> Self {
        let cap = NonZeroUsize::new(1).unwrap();
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(cap));
        scope.enable_registration().unwrap();
        let egress = Arc::new(EgressInterfaceControl::default());
        let binding = UpstreamBinding {
            prefix: vec![],
            dial: Default::default(),
            network_generation: NetworkGeneration(egress.generation()),
        };
        let actual = ComponentInstanceId {
            component: ComponentId(991),
            generation: ResourceGeneration(3),
        };
        let upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(
                NativeCarrierOpener::new(actual, binding.clone(), peer, egress, Some(observer))
                    .unwrap(),
            ),
        );
        let pool = GrpcPool::default();
        let options = Arc::new(
            NativePoolOptions::from_pool(
                &pool,
                NativePoolSettings {
                    server: peer.ip().to_string(),
                    port: peer.port(),
                    grpc: crate::profile::OwnedGrpcProfile {
                        service_names: vec!["owned".into()],
                        multi_mode: true,
                        ..Default::default()
                    },
                    tls: None,
                    dial: binding.dial.clone(),
                },
                cap,
                NonZeroUsize::new(2).unwrap(),
            )
            .unwrap(),
        );
        let owner = NativePoolFactory
            .build(
                options.clone(),
                BuildContext {
                    component: actual.component,
                    generation: actual.generation,
                    dependencies: Arc::from([]),
                    roles: Default::default(),
                    resources: Arc::new(Denied),
                    tasks: scope.registrar(),
                    cancel: CancellationToken::new(),
                    background_carrier: Some(upstream),
                },
            )
            .unwrap();
        Self {
            pool,
            options,
            actual,
            binding,
            scope,
            owner,
        }
    }
    fn demand(&self) -> Demand {
        Demand {
            deadline: deadline(),
            cancel: CancellationToken::new(),
            binding: self.binding.clone(),
        }
    }
}

async fn actual_failure(user_receives: bool) {
    timeout(Duration::from_secs(7), async {
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
                let mut bytes = [0; 4];
                io.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, expected);
                io.write_all(&bytes).await.unwrap();
                io.flush().await.unwrap();
            }
            finished.await.unwrap();
            incoming.close();
        });
        let observer = Arc::new(FailActualRead::default());
        let mut fixture = HostedFixture::new(peer, observer.clone());
        fixture.owner.lifecycle.start(deadline()).await.unwrap();
        let managed = fixture.pool.0.managed(fixture.actual).unwrap();
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
        let physical = managed.records.lock().unwrap()[0].clone();
        observer.0.store(true, Ordering::SeqCst);
        io.write_all(b"fail").await.unwrap();
        let receipt = fixture.scope.next_receipt().await.unwrap();
        let original = receipt.result().unwrap_err();
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        assert!(
            managed.ready(),
            "a failed physical carrier does not revoke its shared component"
        );
        assert!(physical.received.lock().unwrap().is_none());
        assert!(!receipt.local_failure_accepted());
        assert_eq!(
            fixture
                .scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        assert!(physical.adoption.closed().is_none());
        if user_receives {
            let failure = io.read_exact(&mut bytes).await.unwrap_err();
            let shared = failure
                .get_ref()
                .unwrap()
                .downcast_ref::<super::super::stream::SharedFailure>()
                .unwrap();
            assert!(Arc::ptr_eq(&original, &shared.0));
            assert!(
                physical.adoption.closed().is_none(),
                "local receipt is not native closure"
            );
            assert_eq!(
                managed.grants.reserve().err().unwrap().reason,
                Reason::Capacity
            );
            assert!(receipt.local_failure_accepted());
        }
        drop(io);
        // This is the actual owner's no-user cleanup path as well as the
        // ordinary post-user path. It persists the same exit before local ACK.
        let report = fixture.owner.lifecycle.shutdown(deadline()).await.unwrap();
        assert!(report.is_complete(), "{report:?}");
        assert_eq!(report.closed_io.len(), 1);
        let received = physical.received.lock().unwrap().clone().unwrap();
        assert_eq!(received.id(), receipt.id());
        assert_eq!(received.class(), receipt.class());
        let failure = received.result().unwrap_err();
        assert!(Arc::ptr_eq(&original, &failure));
        assert_eq!(failure.stage, original.stage);
        assert_eq!(failure.reason, original.reason);
        assert_eq!(failure.impact, original.impact);
        assert_eq!(failure.component, original.component);
        assert_eq!(failure.session, original.session);
        assert_eq!(failure.delivery, original.delivery);
        assert_eq!(failure.retry, original.retry);
        assert!(physical.closed());
        assert!(receipt.local_failure_accepted());
        assert_eq!(
            fixture
                .scope
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        );
        fixture.scope.acknowledge(&receipt).unwrap();
        let healthy = fixture
            .scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        let success = fixture.scope.next_receipt().await.unwrap();
        assert_eq!(success.id(), healthy);
        assert!(success.result().is_ok());
        fixture.scope.acknowledge(&success).unwrap();
        fixture.scope.begin_shutdown();
        assert!(fixture.scope.next_receipt().await.is_none());
        assert!(fixture.scope.is_drained());
        finish.send(()).unwrap();
        server.await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn hosted_real_grpc_user_receives_same_failure_before_publication_credit_and_native_close() {
    actual_failure(true).await;
}
#[tokio::test]
async fn hosted_real_grpc_no_user_cleanup_accepts_original_failure_without_a_history_queue() {
    actual_failure(false).await;
}
