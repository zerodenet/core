use super::*;
pub(super) struct Ids(pub(super) AtomicU64);
impl SessionIdentitySource for Ids {
    fn allocate(&self) -> Result<SessionId, ChannelError> {
        Ok(SessionId(self.0.fetch_add(1, Ordering::AcqRel)))
    }
    fn allocate_authenticated(
        &self,
        _: Option<zero_core::SessionAuth>,
    ) -> Result<SessionId, ChannelError> {
        self.allocate()
    }
}
struct Resources(Mutex<Option<ControlledTcpListener>>);
impl ResourceAccess for Resources {
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, ChannelError>> {
        Box::pin(async {
            self.0.lock().unwrap().take().ok_or_else(|| {
                ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
            })
        })
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { unreachable!() })
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        Box::pin(async {
            Err(ChannelError::new(
                Stage::Prepare,
                Reason::Unsupported,
                Impact::Component,
            ))
        })
    }
    fn open_device(
        &self,
        _: Arc<str>,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::contract::packet::BoxPacketSession, ChannelError>>
    {
        Box::pin(async { unreachable!() })
    }
}
pub(super) struct Host {
    pub instance: ComponentInstance,
    pub scope: TaskScope,
    pub original: ControlledTcpListenerOwner,
    pub endpoint: SocketAddr,
    pub stop: CancellationToken,
    pub portal: crate::reverse::Portal,
}
impl Host {
    pub async fn new(grpc: bool) -> Self {
        Self::with_request(request(USER, grpc)).await
    }
    pub async fn with_request(request: VlessInboundListenerRequest) -> Self {
        let nz = |n| NonZeroUsize::new(n).unwrap();
        let runtime = crate::transport::VlessTransportRuntime::default();
        let options = Arc::new(crate::component::inbound::listener::Options::new(
            Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
            request.prepare_publication(&runtime, "source"),
            b"complete test identity",
        ));
        let factory = crate::component::inbound::listener::Factory;
        let spec = factory.describe(&options);
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), nz(520));
        scope.enable_registration().unwrap();
        let stop = CancellationToken::new();
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(
            &spec.resources[0],
            nz(1),
            stop.clone(),
            nz(1),
            nz(520),
            &scope.registrar(),
        )
        .unwrap();
        let listener = original
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let endpoint = listener.local_addr().unwrap();
        let mut instance = factory
            .build(
                options,
                BuildContext {
                    component: ComponentId(11),
                    generation: ResourceGeneration(1),
                    dependencies: Arc::from([]),
                    roles: Default::default(),
                    resources: Arc::new(Resources(Mutex::new(Some(listener)))),
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
            original,
            endpoint,
            stop,
            portal: runtime.reverse_portal("portal"),
        }
    }
    pub async fn accept(
        &mut self,
    ) -> (
        TcpStream,
        tokio::sync::oneshot::Receiver<(AcceptedRequest, HandshakeContext)>,
    ) {
        let client = TcpStream::connect(self.endpoint).await.unwrap();
        let pending = self
            .instance
            .roles
            .inbound
            .as_mut()
            .unwrap()
            .next(AcceptContext {
                deadline: None,
                cancel: self.stop.clone(),
            })
            .await
            .unwrap()
            .unwrap();
        let (ready, accepted) = tokio::sync::oneshot::channel();
        let stop = self.stop.clone();
        pending
            .spawn(TaskClass::Auxiliary, move |pending, tasks| async move {
                let context = HandshakeContext {
                    accepted: Some(tasks.caller()?),
                    connection: None,
                    inbound: RoleInstanceId {
                        component: ComponentId(11),
                        generation: ResourceGeneration(1),
                        role: RoleId::Inbound(InboundId(11)),
                    },
                    identities: Arc::new(Ids(AtomicU64::new(701))),
                    deadline: Instant::now() + Duration::from_secs(3),
                    cancel: stop.clone(),
                };
                let request = pending.complete(context.clone()).await?;
                ready.send((request, context)).map_err(|_| {
                    ChannelError::new(Stage::Receive, Reason::Revoked, Impact::Session)
                })?;
                stop.cancelled().await;
                Ok(())
            })
            .unwrap();
        (client, accepted)
    }
    pub async fn close(self, expected_tasks: usize) {
        self.close_at_least(expected_tasks, Some(expected_tasks))
            .await;
    }
    pub async fn close_at_least(mut self, minimum: usize, exact: Option<usize>) {
        self.stop.cancel();
        self.instance
            .lifecycle
            .revoke(zero_platform_tokio::contract::CloseReason::Shutdown);
        self.instance.roles.inbound.take();
        self.scope.begin_shutdown();
        let receiver = self.original.accepted_task_receiver().unwrap();
        let mut joined = 0;
        while let Some(exit) = self.scope.next_exit().await {
            assert!(exit.result.is_ok(), "{:?}", exit.result);
            assert!(receiver.observe_joined(exit.id).is_some());
            joined += 1;
        }
        assert!(joined >= minimum);
        if let Some(exact) = exact {
            assert_eq!(joined, exact);
        }
        let report = self
            .instance
            .lifecycle
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(report.is_complete());
        self.original
            .cancel(zero_platform_tokio::contract::CloseReason::Shutdown);
        let report = self
            .original
            .wait_closed(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(report.is_complete());
        assert_eq!(
            report.closed_io.len(),
            2,
            "original listener and original accepted native carrier"
        );
        assert!(self
            .original
            .accepted_evidence()
            .unwrap()
            .provider_confirmed());
    }
}
