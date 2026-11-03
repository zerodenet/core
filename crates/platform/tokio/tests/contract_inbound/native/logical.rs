//! Sequential logical datagrams reuse bounded original slots while TCP stays live.
use super::*;
use zero_platform_tokio::controlled_datagram::{
    listener::ControlledDatagramListenerControl, ControlledDatagramOwner,
};

struct Native {
    owner: ControlledDatagramOwner,
    control: Option<ControlledDatagramListenerControl>,
    closes: usize,
}
impl Native {
    fn new() -> Self {
        Self {
            owner: ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap()),
            control: None,
            closes: 0,
        }
    }
}
struct Original(Arc<Mutex<Native>>);
impl ResourceLifecycle for Original {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(control) = &self.0.lock().unwrap().control {
            control.revoke();
        }
    }
    async fn shutdown(
        &mut self,
        _: Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        let mut native = self.0.lock().unwrap();
        if let Some(mut original) = native.control.take() {
            original.close().unwrap();
            native.closes += 1;
            return Ok(zero_platform_tokio::contract::lifecycle::CloseReport {
                closed_io: vec!["actual original UDP receipt".into()],
                ..Default::default()
            });
        }
        Ok(Default::default())
    }
}

#[tokio::test]
async fn logical_datagram_reuses_original_custody_only_after_real_driver_join_and_native_close() {
    timeout(Duration::from_secs(5), async {
        let one = NonZeroUsize::new(1).unwrap();
        let two = NonZeroUsize::new(2).unwrap();
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), two);
        let declaration = ResourceDeclaration::TcpListener {
            destination: Destination::new(Address::Ipv4([127,0,0,1]), 0),
            accepted: Some(AcceptedTcpLimits { max_sockets: one, max_tasks_per_socket: two, max_resources_per_socket: 2 }),
            max_bind_attempts: one,
        };
        let mut original = ControlledTcpListenerOwner::new_with_accepted_graph(&declaration, one, CancellationToken::new(), one, two, &scope.registrar()).unwrap();
        let mut listener = original.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (accepted, _) = listener.accept_owned().await.unwrap();
        let pending = accepted.into_pending(|socket| SocketHandshake { socket, started: Arc::default(), pending: Arc::new(Notify::new()) }).unwrap();
        let (claim, receiver) = pending.accepted_tcp().unwrap();
        let release = Arc::new(Notify::new());
        let (send, context) = tokio::sync::oneshot::channel();
        let (primary, _) = pending.spawn(TaskClass::Auxiliary, {
            let release = release.clone();
            move |pending, tasks| async move {
                let _original_view = pending;
                send.send(tasks).ok().unwrap();
                release.notified().await;
                Ok(())
            }
        }).unwrap();
        let tasks = context.await.unwrap();
        let caller = tasks.caller().unwrap();
        let mut historical = Vec::new();
        for _ in 0..2 {
            let state = Arc::new(Mutex::new(Native::new()));
            let resource = Arc::new(caller.retain_logical_resource(zero_platform_tokio::contract::lifecycle::erase_lifecycle(Original(state.clone()))).unwrap());
            let entered = Arc::new(Notify::new());
            let (opened, io) = tokio::sync::oneshot::channel();
            let (finished, body) = tokio::sync::oneshot::channel();
            let (id, witness) = tasks.spawn(TaskClass::Critical, {
                let resource = resource.clone();
                let state = state.clone();
                let entered = entered.clone();
                move |driver| {
                    resource.bind_driver(&driver).unwrap();
                    async move {
                        let (mut rx, tx, control) = state.lock().unwrap().owner.bind_listener("127.0.0.1:0".parse().unwrap(), one).unwrap();
                        let address = rx.local_addr().unwrap();
                        state.lock().unwrap().control = Some(control); // original persists before await
                        opened.send((address, tx)).ok().unwrap();
                        let mut buffer = [0;4];
                        let mut read = Box::pin(rx.recv_from_addr(&mut buffer));
                        poll_fn(|cx| { let result = read.as_mut().poll(cx); if result.is_pending() { entered.notify_one(); } result }).await.unwrap();
                        finished.send(()).unwrap();
                        Ok(())
                    }
                }
            }).unwrap();
            let (address, transferred) = io.await.unwrap();
            entered.notified().await;
            assert!(!witness.is_joined());
            let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            peer.send_to(b"end", address).await.unwrap();
            body.await.unwrap();
            assert!(matches!(resource.close(Instant::now()).await, Err(error) if error.reason == Reason::Timeout));
            assert_eq!(state.lock().unwrap().closes, 0);
            assert!(caller.retain_logical_resource(zero_platform_tokio::contract::lifecycle::erase_lifecycle(Original(Arc::new(Mutex::new(Native::new()))))).is_err(), "body return never refunds original custody");
            // A cancelled/lost caller never drops its retained native owner.
            historical.push(resource.clone());
            drop(resource);
            receiver.retire_logical_resources(Instant::now() + Duration::from_secs(1)).await.unwrap();
            assert_eq!(state.lock().unwrap().closes, 0);
            assert_eq!(scope.next_exit().await.unwrap().id, id);
            receiver.observe_joined(id).unwrap();
            receiver.retire_logical_resources(Instant::now() + Duration::from_secs(1)).await.unwrap();
            assert_eq!(state.lock().unwrap().closes, 1);
            assert!(transferred.send_to_addr(b"closed", peer.local_addr().unwrap()).await.is_err());
            assert!(caller.check_open().is_ok(), "same primary remains live for the next logical request");
        }
        release.notify_one();
        assert_eq!(scope.next_exit().await.unwrap().id, primary);
        receiver.observe_joined(primary).unwrap();
        assert!(receiver.close(&claim, Instant::now() + Duration::from_secs(1)).await.unwrap().is_some());
        drop((client, listener));
        let report = original.wait_closed(Instant::now() + Duration::from_secs(1)).await.unwrap();
        assert!(report.is_complete());
        assert!(report.closed_io.iter().any(|io| io.contains("2 native I/O close receipts")), "aggregate the two original closes at one finite ordinal");
        drop(original);
        for resource in historical { assert!(resource.close(Instant::now() + Duration::from_secs(1)).await.unwrap()); }
        // The weak observation retains only the settled logical-debt count;
        // native TCP closure above came from its actual original owner.
        receiver.retire_logical_resources(Instant::now() + Duration::from_secs(1)).await.unwrap();
    }).await.unwrap();
}
