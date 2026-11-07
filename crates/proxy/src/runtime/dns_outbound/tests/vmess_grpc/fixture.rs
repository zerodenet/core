use super::*;
use zero_platform_tokio::controlled_carrier::CarrierOrigin;
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub(super) fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(3)
}

#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("qualified gRPC DNS must use its actual standard owner") })
    }
}
pub(super) struct Peer {
    pub(super) address: std::net::SocketAddr,
    accepted: Arc<AtomicUsize>,
    stop: CancellationToken,
    worker: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub(super) async fn new(multi: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let worker = tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = tokio::select! { biased;
                    _ = stopping.cancelled() => break,
                    socket = listener.accept() => socket.unwrap(),
                };
                count.fetch_add(1, Ordering::SeqCst);
                let stopped = stopping.clone();
                peers.spawn(async move {
                    let incoming = zero_transport::grpc::accept_grpc_connection(
                        zero_platform_tokio::TcpRelayStream::new(socket),
                        &zero_transport::profile::OwnedGrpcProfile { service_names: vec!["actual.dns/Tun".into()], multi_mode: multi, ..Default::default() },
                    ).unwrap();
                    let io = incoming.accept().await.unwrap();
                    let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
                        ID.into(), "aes-128-gcm".into(), None::<String>, None, None, None, None, None,
                    )]).unwrap();
                    let (facts, mut io) = profile.accept_request_stream(vmess::inbound::VmessInbound, zero_platform_tokio::TcpRelayStream::new(io)).await.unwrap();
                    let zero_core::Address::Ipv4(ip) = facts.destination.address else { panic!("actual literal DNS destination"); };
                    let mut dns = tokio::net::TcpStream::connect((std::net::Ipv4Addr::from(ip), facts.destination.port)).await.unwrap();
                    tokio::select! { _ = stopped.cancelled() => {}, _ = tokio::io::copy_bidirectional(&mut io, &mut dns) => {} }
                    // Peer Drop is never evidence of the client's H2/native
                    // closure. Retain this actual server connection until its
                    // client controller receipts have been asserted.
                    stopped.cancelled().await;
                    incoming.close();
                });
            }
            while let Some(peer) = peers.join_next().await {
                peer.unwrap();
            }
        });
        Self {
            address,
            accepted,
            stop,
            worker,
        }
    }
    pub(super) fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.worker.await.unwrap();
    }
}

pub(super) fn config(
    address: std::net::SocketAddr,
    multi: bool,
    query: &crate::dns_fixture::Dns,
    node: Option<&crate::dns_fixture::Dns>,
) -> zero_config::RuntimeConfig {
    let mut dns = query.config();
    let mut server = query.server();
    let zero_config::DnsServerConfig::Udp { detour, .. } = &mut server else {
        unreachable!()
    };
    *detour = Some("grpc".into());
    dns.servers.insert("query".into(), server);
    dns.default_server = "query".into();
    dns.policy.node_server = Some("native".into());
    if let Some(node) = node {
        dns.servers.insert("native".into(), node.server());
    }
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime": {"dns": dns},
        "outbounds": [{"tag":"grpc", "protocol":{"type":"vmess", "server": if node.is_some() {"grpc.actual.node.test"} else {"127.0.0.1"}, "port":address.port(), "id":ID, "cipher":"aes-128-gcm", "grpc":{"service_names":["actual.dns/Tun"],"multi_mode":multi}}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
pub(super) async fn start(
    config: zero_config::RuntimeConfig,
) -> (Proxy, crate::RunningProxy, crate::ProxyHandle) {
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
    let running = proxy.spawn();
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running, handle)
}
pub(super) fn query(
    proxy: &Proxy,
    name: &str,
) -> tokio::task::JoinHandle<std::io::Result<Vec<IpAddress>>> {
    let resolver = proxy.resolver.clone();
    let name = name.to_owned();
    tokio::spawn(async move { resolver.resolve_real_type(&name, 1).await })
}
pub(super) async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout_at(deadline(), async {
        loop {
            if let Some(query) =
                proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| {
                        query.io_pending && query.original.is_some() && !query.task.is_joined()
                    })
            {
                return query;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual DNS query suspended in native I/O")
}
pub(super) fn originals(
    caller: &zero_dns::OwnedDnsQueryObservation,
) -> (CarrierOrigin, CarrierOrigin) {
    let child = caller.original.clone().unwrap();
    let original = child
        .parent()
        .expect("actual per-connection gRPC donor")
        .clone();
    assert!(original.parent().is_none());
    let driver = original.driver().expect("actual per-connection H2 driver");
    let zero_platform_tokio::controlled_carrier::CarrierOriginOwner::Carrier(owner) =
        original.owner()
    else {
        panic!("actual ordinary Node carrier owner");
    };
    let observed = owner.observe().unwrap();
    let slot = observed
        .slots()
        .iter()
        .find(|slot| {
            slot.driver()
                .is_some_and(|(_, actual)| actual.same_registration(driver))
        })
        .expect("same original driver's registered slot");
    let auxiliary = slot.auxiliary().expect("same original Node preparation");
    assert!(auxiliary
        .scope_identity()
        .same_scope(&driver.scope_identity()));
    assert!(auxiliary
        .scope_identity()
        .same_scope(&owner.bound().unwrap().scope_identity()));
    let joined = auxiliary
        .try_joined()
        .expect("real preparation finished before delivery");
    assert!(auxiliary.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    (child, original)
}
pub(super) fn closed(child: &CarrierOrigin, original: &CarrierOrigin) {
    let logical = child.closed().expect("same actual DNS logical close");
    assert!(logical.matches(child) && !logical.had_native_acquisition());
    assert!(original.driver().unwrap().is_joined());
    let native = original.closed().expect("same actual gRPC native close");
    assert!(native.matches(original) && native.had_native_acquisition());
}
