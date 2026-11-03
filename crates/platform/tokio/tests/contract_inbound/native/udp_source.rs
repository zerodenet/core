//! Real native UDP admissions never borrow a TCP claim or equate view return with close.
use super::*;
use zero_platform_tokio::{
    contract::{component::DatagramPeerLimits, inbound::udp, lifecycle::ChannelControl},
    controlled_datagram::ControlledDatagramOwner,
};

struct DatagramResources(Mutex<Option<GrantedDatagramListener>>);
impl ResourceAccess for DatagramResources {
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, BuildError>> {
        Box::pin(async { Ok(self.0.lock().unwrap().take().expect("one actual original")) })
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<ControlledTcpListener, BuildError>> {
        panic!("native peer must not invent TCP")
    }
    fn read_file(&self, _: PathBuf, _: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        panic!("undeclared file")
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        panic!("undeclared device")
    }
}
struct Cold;
impl InboundHandshake for Cold {
    async fn complete(self, _: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        panic!("source admission must not run business preparation")
    }
}

#[tokio::test]
async fn native_udp_source_retains_original_identity_and_blocks_reuse_until_peer_cleanup() {
    timeout(Duration::from_secs(3), async {
        let one = NonZeroUsize::new(1).unwrap();
        let limits = DatagramPeerLimits {
 max_tasks_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
 max_resources_per_peer: 0, max_peers: one, packets_per_peer: one };
        let mut original = ControlledDatagramOwner::new(one);
        let (rx, tx, mut control) = original.bind_listener("127.0.0.1:0".parse().unwrap(), one).unwrap();
        let endpoint = rx.local_addr().unwrap();
        let identity = tx.identity();
        let retained = tx.clone();
        let resources = Arc::new(DatagramResources(Mutex::new(Some(GrantedDatagramListener { rx, tx }))));
        let peer = Arc::new(Mutex::new(None));
        let (mut owner, mut source) = udp::source(
            Destination::new(Address::Ipv4([127,0,0,1]), 0), resources,
            CancellationToken::new(), limits, Duration::from_secs(120), {
                let peer = peer.clone();
                move |socket| { *peer.lock().unwrap() = Some(socket); Cold }
            },
        );
        owner.start(Instant::now() + Duration::from_secs(1)).await.unwrap();
        let cancelled = CancellationToken::new();
        let mut waiting = Box::pin(source.next(AcceptContext { deadline: None, cancel: cancelled.clone() }));
        poll_fn(|cx| match waiting.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("actual native receive must still be pending"),
        }).await;
        cancelled.cancel();
        assert!(matches!(waiting.await, Err(error) if error.reason == Reason::Cancelled));
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"", endpoint).await.unwrap();
        let pending = source.next(AcceptContext { deadline: None, cancel: CancellationToken::new() }).await.unwrap().unwrap();
        assert!(pending.accepted_tcp().is_none(), "no synthetic TCP authority");
        let admission = pending.datagram_peer().unwrap();
        assert!(admission.matches(&identity, limits));
        assert_eq!(pending.connection_facts().unwrap().effective_source, Some(client.local_addr().unwrap()));
        assert_eq!(pending.connection_facts().unwrap().local_endpoint, Some(endpoint));
        let retirement = admission.retirement();
        let (send, mut receive, mut peer_control) = peer.lock().unwrap().take().unwrap().split();
        assert_eq!(poll_fn(|cx| receive.poll_recv(cx)).await.unwrap(), Some(Vec::new()), "empty packet is not EOF");
        send.send(b"reply").await.unwrap();
        let mut buffer = [0;32];
        let (size, sender) = client.recv_from(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..size], b"reply");
        assert_eq!(sender, endpoint);
        drop(send);
        drop(receive);
        drop(pending);
        assert!(peer_control.wait_closed(Instant::now() + Duration::from_secs(1)).await.unwrap().is_complete());
        assert!(!retirement.is_released(), "logical view return does not close retained peer originals");
        client.send_to(b"early", endpoint).await.unwrap();
        assert!(matches!(source.next(AcceptContext {
            deadline: Some(Instant::now() + Duration::from_millis(30)), cancel: CancellationToken::new(),
        }).await, Err(error) if error.reason == Reason::Timeout));
        // This fixture acquired no peer egress resources or tasks; all original
        // peer views above have returned. Only the host releases this admission.
        retirement.release_after_cleanup();
        client.send_to(b"new", endpoint).await.unwrap();
        let replacement = source.next(AcceptContext { deadline: None, cancel: CancellationToken::new() }).await.unwrap().unwrap();
        assert!(replacement.datagram_peer().unwrap().matches(&identity, limits));
        let (send, mut receive, _) = peer.lock().unwrap().take().unwrap().split();
        assert_eq!(poll_fn(|cx| receive.poll_recv(cx)).await.unwrap(), Some(b"new".to_vec()));
        drop(send);
        drop(receive);
        owner.shutdown(Instant::now() + Duration::from_secs(1)).await.unwrap();
        // Source/view cleanup is not the shared native listener close receipt.
        retained.send_to_addr(b"still original", client.local_addr().unwrap()).await.unwrap();
        let receipt = control.close().unwrap();
        assert_eq!(receipt.reservation(), control.reservation());
        assert!(retained.send_to_addr(b"retired", client.local_addr().unwrap()).await.is_err());
    }).await.unwrap();
}

#[tokio::test]
async fn native_peer_scope_uses_real_registration_and_retains_originals_until_host_joins() {
    timeout(Duration::from_secs(5), async {
        let nz = |n| NonZeroUsize::new(n).unwrap();
        let limits = DatagramPeerLimits { max_peers:nz(1), packets_per_peer:nz(2), max_tasks_per_peer:nz(2), max_resources_per_peer:1 };
        let mut native = ControlledDatagramOwner::new(nz(1));
        let (rx,tx,mut control)=native.bind_listener("127.0.0.1:0".parse().unwrap(),nz(2)).unwrap();
        let endpoint=rx.local_addr().unwrap();
        let retained=tx.clone();
        let slot=Arc::new(Mutex::new(None));
        let (mut source_owner,mut source)=udp::source(Destination::new(Address::Ipv4([127,0,0,1]),0),Arc::new(DatagramResources(Mutex::new(Some(GrantedDatagramListener{rx,tx})))),CancellationToken::new(),limits,Duration::from_secs(120),{
            let slot=slot.clone(); move |peer| { *slot.lock().unwrap()=Some(peer); Cold }
        });
        source_owner.start(Instant::now()+Duration::from_secs(1)).await.unwrap();
        let remote=tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap(); remote.send_to(b"first",endpoint).await.unwrap();
        let pending=source.next(AcceptContext{deadline:None,cancel:CancellationToken::new()}).await.unwrap().unwrap();
        assert!(pending.accepted_tcp().is_none());
        let admission=pending.datagram_peer().unwrap().clone();
        let (send,mut receive,_)=slot.lock().unwrap().take().unwrap().split();
        let mut scope=TaskScope::new(tokio::runtime::Handle::current(),nz(3)); scope.enable_registration().unwrap();
        let mut foreign=TaskScope::new(tokio::runtime::Handle::current(),nz(1)); foreign.enable_registration().unwrap();
        let stop=CancellationToken::new(); let stopping=stop.clone();
        let suspended=Arc::new(Notify::new()); let reached=suspended.clone();
        let (id,witness)=scope.registrar().spawn_tracked(TaskClass::Critical,async move {
            assert_eq!(poll_fn(|cx| receive.poll_recv(cx)).await.unwrap(),Some(b"first".to_vec()));
            tokio::select! { biased;
                _=stopping.cancelled()=>{},
                _=poll_fn(|cx| {let result=receive.poll_recv(cx); if result.is_pending(){reached.notify_one();} result})=>{},
            }
            drop(send); drop(receive); drop(pending); Ok(())
        }).unwrap();
        suspended.notified().await;
        assert!(admission.bind_scope(&foreign.registrar().downgrade(),id,witness.clone(),stop.clone()).is_err());
        assert!(admission.bind_scope(&scope.registrar().without_tasks().downgrade(),id,witness.clone(),stop.clone()).is_err(),"taskless view cannot mint an original task/resource ledger");
        let mut peer=admission.bind_scope(&scope.registrar().downgrade(),id,witness.clone(),stop.clone()).unwrap();
        assert!(admission.bind_scope(&scope.registrar().downgrade(),id,witness.clone(),stop.clone()).is_err(),"one native peer budget, never two graphs");
        let caller=peer.tasks().caller().unwrap(); assert_eq!(caller.origin().unwrap(),zero_platform_tokio::AcceptedOrigin::Datagram);
        let authority=zero_platform_tokio::contract::component::AcceptedDatagramAuthority::new(&ResourceDeclaration::AcceptedDatagram{max_sockets:nz(1),max_pending_sends:nz(1)},&scope.registrar(),CancellationToken::new()).unwrap();
        let egress=authority.open(&caller).unwrap(); let mut egress_rx=egress.rx; let escaped=egress.tx.clone();
        let child_ready=Arc::new(Notify::new()); let reached=child_ready.clone();
        let (child,child_witness)=peer.tasks().spawn(TaskClass::Auxiliary,move |_|async move {
            let mut bytes=[0;16];
            let receive = egress_rx.recv_from_addr(&mut bytes);
            tokio::pin!(receive);
            poll_fn(|cx| {
                let result=receive.as_mut().poll(cx); if result.is_pending(){reached.notify_one();} result
            }).await.ok();
            drop(egress.tx); Ok(())
        }).unwrap();
        child_ready.notified().await;
        assert!(peer.tasks().spawn(TaskClass::Auxiliary,|_|async {Ok(())}).is_err(),"finite original peer task budget");
        let first=peer.shutdown(Instant::now()).await.unwrap(); assert!(!first.is_complete()); assert!(!admission.retirement().is_released());
        assert!(!witness.is_joined()); assert!(!child_witness.is_joined());
        for _ in 0..2 { let exit=scope.next_exit().await.unwrap(); assert!(exit.id==id||exit.id==child); assert!(peer.observe_joined(exit.id).is_some()); }
        let report=peer.shutdown(Instant::now()+Duration::from_secs(1)).await.unwrap();
        assert!(report.is_complete()); assert_eq!(report.closed_io.len(),1,"original egress provider receipt, not root native UDP closure");
        assert!(admission.retirement().is_released()); assert!(caller.check_open().is_err());
        assert!(escaped.send_to_addr(b"retired",remote.local_addr().unwrap()).await.is_err());
        retained.send_to_addr(b"root still owned",remote.local_addr().unwrap()).await.unwrap();
        source_owner.shutdown(Instant::now()+Duration::from_secs(1)).await.unwrap();
        let closed=control.close().unwrap(); assert_eq!(closed.reservation(),control.reservation());
        assert!(retained.send_to_addr(b"closed",remote.local_addr().unwrap()).await.is_err());
        let identity = caller.scope_identity();
        drop(scope);
        assert!(!identity.resource_open(), "retired peer owner and caller retain only weak Scope authority");
    }).await.unwrap();
}
