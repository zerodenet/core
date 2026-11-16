//! Minimal real host: original UDP grant, actual peer Scope and native receipt.
use super::*;
use zero_platform_tokio::contract::lifecycle::ResourceLifecycle;
use zero_platform_tokio::{
    controlled_datagram::{
        listener::{ControlledDatagramListenerControl, ControlledDatagramListenerTx},
        ControlledDatagramOwner,
    },
    AcceptedPeerScope,
};
struct Resources(Mutex<Option<GrantedDatagramListener>>);
impl ResourceAccess for Resources {
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        Box::pin(async { Ok(self.0.lock().unwrap().take().unwrap()) })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, ChannelError>> {
        panic!("mKCP never borrows TCP admission")
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        panic!("undeclared file")
    }
    fn open_device(
        &self,
        _: Arc<str>,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::packet::BoxPacketSession, ChannelError>>
    {
        panic!("undeclared device")
    }
}
pub(super) struct Host {
    pub instance: ComponentInstance,
    pub scope: TaskScope,
    pub endpoint: SocketAddr,
    pub stop: CancellationToken,
    original: ControlledDatagramOwner,
    control: ControlledDatagramListenerControl,
    retained: ControlledDatagramListenerTx,
}
impl Host {
    pub async fn new(request: VlessInboundListenerRequest) -> Self {
        let nz = |n| NonZeroUsize::new(n).unwrap();
        let runtime = crate::transport::VlessTransportRuntime::default();
        let factory = crate::component::inbound::listener::Factory;
        let options = Arc::new(crate::component::inbound::listener::Options::new(
            Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            request.prepare_publication(&runtime, "peer"),
            b"real mKCP test source",
        ));
        assert!(matches!(
            factory.describe(&options).resources[0],
            ResourceDeclaration::DatagramListener {
                accepted: Some(_),
                ..
            }
        ));
        let mut original = ControlledDatagramOwner::new(nz(1));
        let (rx, tx, control) = original
            .bind_listener("127.0.0.1:0".parse().unwrap(), nz(32))
            .unwrap();
        let endpoint = rx.local_addr().unwrap();
        let retained = tx.clone();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), nz(521));
        scope.enable_registration().unwrap();
        let stop = CancellationToken::new();
        let mut instance = factory
            .build(
                options,
                BuildContext {
                    component: ComponentId(12),
                    generation: ResourceGeneration(1),
                    dependencies: Arc::from([]),
                    roles: Default::default(),
                    resources: Arc::new(Resources(Mutex::new(Some(GrantedDatagramListener {
                        rx,
                        tx,
                    })))),
                    tasks: scope.registrar().without_tasks(),
                    cancel: stop.clone(),
                    background_carrier: None,
                },
            )
            .unwrap();
        instance
            .lifecycle
            .start(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        Self {
            instance,
            scope,
            endpoint,
            stop,
            original,
            control,
            retained,
        }
    }
    pub async fn accept(&mut self) -> (AcceptedRequest, HandshakeContext, AcceptedPeerScope) {
        let mut source = self.instance.roles.inbound.take().unwrap();
        let pending = source
            .next(AcceptContext {
                deadline: None,
                cancel: self.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        assert!(pending.accepted_tcp().is_none());
        let admission = pending.datagram_peer().unwrap().clone();
        let (tasks_tx, tasks_rx) =
            tokio::sync::oneshot::channel::<zero_platform_tokio::AcceptedTasks>();
        let (ready, accepted) = tokio::sync::oneshot::channel();
        let stop = self.stop.clone();
        let (id, witness) = self
            .scope
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let tasks = tasks_rx.await.unwrap();
                let context = HandshakeContext {
                    accepted: Some(tasks.caller()?),
                    connection: None,
                    inbound: RoleInstanceId {
                        component: ComponentId(12),
                        generation: ResourceGeneration(1),
                        role: RoleId::Inbound(InboundId(12)),
                    },
                    identities: Arc::new(fixture::Ids(AtomicU64::new(801))),
                    deadline: Instant::now() + Duration::from_secs(3),
                    cancel: stop.clone(),
                };
                let request = pending.complete(context.clone()).await?;
                let _ = ready.send((request, context));
                stop.cancelled().await;
                Ok(())
            })
            .unwrap();
        let peer = admission
            .bind_scope(
                &self.scope.registrar().downgrade(),
                id,
                witness,
                self.stop.clone(),
            )
            .unwrap();
        assert!(tasks_tx.send(peer.tasks()).is_ok());
        let stop = self.stop.clone();
        self.scope
            .registrar()
            .spawn(TaskClass::Auxiliary, async move {
                match source
                    .next(AcceptContext {
                        deadline: None,
                        cancel: stop.clone(),
                    })
                    .await
                {
                    Err(error) if error.reason == Reason::Cancelled => Ok(()),
                    Ok(None) => Ok(()),
                    Ok(Some(_)) => panic!("one actual peer fixture"),
                    Err(error) => Err(error),
                }
            })
            .unwrap();
        let (request, context) = accepted.await.unwrap();
        (request, context, peer)
    }
    pub async fn close(mut self, mut peer: AcceptedPeerScope, expected_peer_tasks: usize) {
        self.stop.cancel();
        assert!(
            !peer.shutdown(Instant::now()).await.unwrap().is_complete(),
            "cancellation without the original Scope joins is incomplete"
        );
        self.scope.begin_shutdown();
        let mut joined = 0;
        let mut peer_joined = 0;
        while let Some(exit) = self.scope.next_exit().await {
            assert!(exit.result.is_ok(), "{:?}", exit.result);
            peer_joined += usize::from(peer.observe_joined(exit.id).is_some());
            joined += 1;
        }
        assert_eq!(
            joined,
            expected_peer_tasks + 1,
            "root source, original peer caller and original conversation driver"
        );
        assert_eq!(peer_joined, expected_peer_tasks);
        assert!(peer
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .is_complete());
        assert!(self
            .instance
            .lifecycle
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap()
            .is_complete());
        assert_eq!(
            self.original.take_report().retained_live,
            1,
            "logical peer cleanup never closes the native original"
        );
        let receipt = self.control.close().unwrap();
        assert_eq!(receipt.reservation(), self.control.reservation());
        assert!(self
            .retained
            .send_to_addr(b"retired", self.endpoint)
            .await
            .is_err());
        let report = self.original.take_report();
        assert_eq!(report.confirmed_closed, 1);
        assert_eq!(report.retained_live, 0);
        assert!(report.unconfirmed.is_empty());
    }
}
