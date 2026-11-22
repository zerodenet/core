use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use zero_platform_tokio::contract::CancellationToken;
pub(super) struct Peer {
    pub(super) address: std::net::SocketAddr,
    accepts: Arc<AtomicUsize>,
    stop: CancellationToken,
    worker: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub(super) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let count = accepts.clone();
        let worker = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = tokio::select! {biased;_=cancel.cancelled()=>break,result=listener.accept()=>result.unwrap()};
                count.fetch_add(1, Ordering::SeqCst);
                let done = cancel.clone();
                connections.spawn(async move {
                    let incoming = zero_transport::grpc::accept_grpc_connection(
                        socket,
                        &zero_transport::profile::OwnedGrpcProfile {
                            service_names: vec!["mode/Tun".into()],
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    let io = incoming.accept().await.unwrap();
                    let profile = vmess::inbound::VmessInboundProfile::from_config_users([(
                        ID.into(),
                        "aes-128-gcm".into(),
                        None::<String>,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )])
                    .unwrap();
                    let (facts, mut io) = profile
                        .accept_request_stream(
                            vmess::inbound::VmessInbound,
                            zero_platform_tokio::TcpRelayStream::new(io),
                        )
                        .await
                        .unwrap();
                    let echo = async {
                        if facts.destination.port == 53 {
                            while let Ok(length) = io.read_u16().await {
                                let mut data = vec![0; length as usize];
                                io.read_exact(&mut data).await.unwrap();
                                assert_eq!(&data[..7], &[1, 203, 0, 113, 9, 0, 53]);
                                assert_eq!(&data[7..], b"live");
                                io.write_u16(length).await.unwrap();
                                io.write_all(&data).await.unwrap();
                                io.flush().await.unwrap();
                            }
                            return;
                        }
                        assert_eq!(facts.destination.port, 443);
                        loop {
                            let mut data = [0; 4];
                            if io.read_exact(&mut data).await.is_err() {
                                break;
                            }
                            assert_eq!(&data, b"live");
                            io.write_all(&data).await.unwrap();
                            io.flush().await.unwrap();
                        }
                    };
                    tokio::select! {_=done.cancelled()=>{},_=echo=>{}}
                    done.cancelled().await;
                    incoming.close();
                });
            }
            while let Some(joined) = connections.join_next().await {
                joined.unwrap();
            }
        });
        Self {
            address,
            accepts,
            stop,
            worker,
        }
    }
    pub(super) fn count(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.worker.await.unwrap();
    }
}
pub(super) fn config(peer: &Peer, compatibility: bool) -> zero_config::RuntimeConfig {
    let mut config = serde_json::json!({
        "outbounds":[
            {"tag":"node","protocol":{"type":"vmess","server":"127.0.0.1","port":peer.address.port(),"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["mode/Tun"]}}}
        ],
        "route":{"final":{"type":"route","outbound":"node"}}
    });
    if compatibility {
        // This Node DNS detour would itself need Node resolution. The domain
        // peer makes the full configuration ineligible for a native Stream
        // role, while the literal peer still has its standard Datagram role.
        config["outbounds"].as_array_mut().unwrap().extend([
            serde_json::json!({"tag":"domain-node","protocol":{"type":"vmess","server":"vpn.example","port":443,"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["mode/Tun"]}}}),
            serde_json::json!({"tag":"bootstrap","protocol":{"type":"socks5","server":"localhost","port":1080}}),
        ]);
        config["runtime"] = serde_json::json!({"dns":{
            "servers":{"remote":{"type":"udp","host":"127.0.0.1","detour":"bootstrap"}},
            "default_server":"remote","policy":{"node_server":"remote"}
        }});
    }
    zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()
}
pub(super) async fn start(
    config: zero_config::RuntimeConfig,
) -> (Proxy, crate::RunningProxy, crate::ProxyHandle) {
    let proxy = Proxy::new(config.clone()).unwrap();
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
async fn open_tcp(
    proxy: &Proxy,
    tag: &str,
) -> Result<crate::transport::EstablishedTcpOutbound, crate::transport::TcpOutboundFailure> {
    let session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        443,
        Network::Tcp,
        ProtocolType::new("test"),
    );
    let services = proxy.tcp_runtime_services().with_service_invocation(
        crate::component::direct_bridge::ServiceInvocation {
            caller: None,
            deadline: Instant::now() + Duration::from_secs(5),
            purpose: zero_core::request::ServicePurpose::DiagnosticProbe,
        },
    );
    let target = services.snapshot().plan().target_id(tag).unwrap();
    let (resolved, _) = services
        .engine()
        .resolve_target_id_in_snapshot(services.snapshot(), target)
        .unwrap();
    crate::runtime::tcp_dispatch::dispatch_tcp_outbound(
        services,
        &session,
        resolved,
        RouteMode::Flow,
        crate::runtime::tcp_dispatch::TcpDispatchIntent::DiagnosticProbe,
    )
    .await
}

pub(super) async fn open(
    proxy: &Proxy,
    tag: &str,
) -> zero_platform_tokio::contract::stream::BoxStream {
    open_tcp(proxy, tag)
        .await
        .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error))
        .into_stream()
        .unwrap()
}

pub(super) async fn assert_tcp_unavailable(proxy: &Proxy) {
    let Err(error) = open_tcp(proxy, "node").await else {
        panic!("unqualified TCP must not open a compatibility carrier");
    };
    assert_eq!(error.stage, "data_plane_route");
}

pub(super) async fn echo(io: &mut zero_platform_tokio::contract::stream::BoxStream) {
    io.write_all(b"live").await.unwrap();
    io.flush().await.unwrap();
    let mut data = [0; 4];
    io.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"live");
}
