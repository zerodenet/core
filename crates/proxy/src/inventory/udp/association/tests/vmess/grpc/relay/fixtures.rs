use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::CancellationToken;
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Socks,
    Vmess,
    Reject,
}
pub(super) struct Prefix {
    pub address: std::net::SocketAddr,
    pub count: Arc<AtomicUsize>,
    requests: mpsc::UnboundedReceiver<Destination>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    kind: Kind,
    response: tokio::sync::watch::Sender<bool>,
}
impl Prefix {
    pub async fn new(kind: Kind, hold: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, requests) = mpsc::unbounded_channel();
        let (response, reply) = tokio::sync::watch::channel(false);
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let accepted = count.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {biased;_=cancel.cancelled()=>break,ready=listener.accept()=>{
                    let (socket,_)=ready.unwrap();accepted.fetch_add(1,Ordering::SeqCst);let (tx,cancel,mut reply)=(tx.clone(),cancel.clone(),reply.clone());
                    tasks.spawn(async move {
                        let work=async {
                            let socket=zero_platform_tokio::TokioSocket::new(socket);
                            let (target,mut socket)=match kind {
                                Kind::Socks | Kind::Reject=>{
                                    let mut socket=socket;
                                    let facts=::socks5::Socks5Inbound.accept_request(&mut socket).await.unwrap();
                                    let target=Destination::new(facts.target,facts.port);tx.send(target.clone()).unwrap();
                                    if hold && matches!(kind,Kind::Reject) {
                                        reply.wait_for(|ready|*ready).await.unwrap();
                                    } else if hold {
                                        // Actual client has submitted CONNECT. No successful reply is fabricated.
                                        let mut byte=[0];let _=socket.read(&mut byte).await;return;
                                    }
                                    if matches!(kind,Kind::Reject) {
                                        ::socks5::Socks5Inbound.send_blocked_response(&mut socket).await.unwrap();
                                        let mut byte=[0];let _=socket.read(&mut byte).await;return;
                                    }
                                    ::socks5::Socks5Inbound.send_success_response(&mut socket).await.unwrap();
                                    (target,zero_platform_tokio::TcpRelayStream::from_client(socket))
                                },
                                Kind::Vmess=>{
                                    let profile=vmess::inbound::VmessInboundProfile::from_config_users([(ID.into(),"aes-128-gcm".into(),None::<String>,None,None,None,None,None)]).unwrap();
                                    let (facts,stream)=profile.accept_request_stream(vmess::inbound::VmessInbound,zero_platform_tokio::TcpRelayStream::from_client(socket)).await.unwrap();
                                    tx.send(facts.destination.clone()).unwrap();(facts.destination,zero_platform_tokio::TcpRelayStream::new(stream))
                                }
                            };
                            assert!(matches!(target.address,Address::Domain(_)),"keep the existing named-target relay wire form");
                            let mut upstream=tokio::net::TcpStream::connect(("127.0.0.1",target.port)).await.unwrap();
                            let _=tokio::io::copy_bidirectional(&mut socket,&mut upstream).await;
                        };
                        tokio::select!{biased;_=cancel.cancelled()=>{},_=work=>{}}
                    });
                }}
                while let Some(done) = tasks.try_join_next() {
                    done.unwrap();
                }
            }
            while let Some(done) = tasks.join_next().await {
                done.unwrap();
            }
        });
        Self {
            address,
            count,
            requests,
            stop,
            task,
            kind,
            response,
        }
    }
    pub fn outbound(&self, tag: &str) -> zero_config::OutboundConfig {
        serde_json::from_value(serde_json::json!({"tag":tag,"protocol":match self.kind {
            Kind::Socks | Kind::Reject=>serde_json::json!({"type":"socks5","server":"127.0.0.1","port":self.address.port()}),
            Kind::Vmess=>serde_json::json!({"type":"vmess","server":"127.0.0.1","port":self.address.port(),"id":ID,"cipher":"aes-128-gcm"}),
        }})).unwrap()
    }
    pub fn release_response(&self) {
        self.response.send_replace(true);
    }
    pub async fn request(&mut self) -> Destination {
        timeout(Duration::from_secs(4), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
pub(super) fn configure(
    config: &mut zero_config::RuntimeConfig,
    prefixes: &[zero_config::OutboundConfig],
    selector: bool,
) {
    config.outbounds.extend_from_slice(prefixes);
    let mut chain: Vec<_> = prefixes.iter().map(|v| v.tag.clone()).collect();
    chain.push("node".into());
    config.outbound_groups.push(
        serde_json::from_value(serde_json::json!({"tag":"chain","type":"relay","proxies":chain}))
            .unwrap(),
    );
    let target = if selector {
        config.outbound_groups.push(
            serde_json::from_value(
                serde_json::json!({"tag":"selected-chain","type":"selector","outbounds":["chain"]}),
            )
            .unwrap(),
        );
        "selected-chain"
    } else {
        "chain"
    };
    config.route =
        serde_json::from_value(serde_json::json!({"final":{"type":"route","outbound":target}}))
            .unwrap();
}
pub(super) fn assert_standard_relay(proxy: &Proxy) {
    let snapshot = proxy.engine().runtime_snapshot();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("chain".into()))
        .unwrap();
    let session = Session::new(
        7,
        Address::Ipv4([203, 0, 113, 9]),
        53,
        Network::Udp,
        ProtocolType::new("test"),
    );
    let prepared = proxy
        .protocols
        .prepare_udp_outbound(
            UdpAdapterContext::new(snapshot.config(), proxy.udp_runtime_services()),
            &session,
            &resolved,
            b"test",
            RouteMode::Flow,
        )
        .unwrap_or_else(|failure| panic!("{}", failure.error));
    assert!(
        matches!(
            prepared,
            crate::inventory::PreparedUdpOutbound::DatagramRelay(_)
        ),
        "the actual route must bypass claim/prepared relay operations"
    );
}
pub(super) async fn close_flow(proxy: &Proxy, dispatch: crate::runtime::udp_dispatch::UdpDispatch) {
    dispatch.finish_all();
    let report = proxy
        .tcp_runtime_services()
        .execution()
        .retired_datagrams()
        .progress_retired(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
}

pub(super) fn donor_owner(
    proxy: &Proxy,
    tag: &str,
) -> zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration {
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let slot = plan.configured(tag).unwrap();
    let ordinal = slot
        .recipe
        .spec()
        .resources
        .iter()
        .position(|resource| {
            matches!(resource,
        zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool{..})
        })
        .unwrap();
    slot.original_carrier_owner(ordinal).unwrap()
}
