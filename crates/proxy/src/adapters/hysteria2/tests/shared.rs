//! Actual Proxy TCP/UDP/packet-path dispatch over the same authenticated peer.
use crate::Proxy;
use ::hysteria2::transport::{
    Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindOptionsRef, Hysteria2InboundBindPlan,
    Hysteria2InboundOptionsRef, Hysteria2InboundUserRef,
};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    Address, InboundClientResponse, InboundDatagramMultiplexer, InboundStreamMultiplexer, Network,
    ProtocolType, Session,
};
use zero_engine::RouteDecision;
fn end() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
struct Server {
    port: u16,
    authenticated: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    closed_changed: Arc<tokio::sync::Notify>,
    wire: Arc<Mutex<HashMap<(Address, u16), u64>>>,
    sessions: Arc<Mutex<BTreeSet<u32>>>,
    stop: tokio::sync::oneshot::Sender<()>,
    retire: tokio::sync::mpsc::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    _material: tempfile::TempDir,
}
impl Server {
    async fn new() -> Self {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("cert.pem"), cert.cert.pem()).unwrap();
        std::fs::write(
            material.path().join("key.pem"),
            cert.signing_key.serialize_pem(),
        )
        .unwrap();
        let endpoint = Hysteria2InboundBindPlan::from_options_refs(
            Some(material.path()),
            Hysteria2InboundBindOptionsRef {
                cert_path: Some("cert.pem"),
                key_path: Some("key.pem"),
            },
        )
        .bind("127.0.0.1:0")
        .await
        .unwrap();
        let port = endpoint.local_addr().unwrap().port();
        let authenticated = Arc::new(AtomicUsize::new(0));
        let count = authenticated.clone();
        let closed = Arc::new(AtomicUsize::new(0));
        let observed_closed = closed.clone();
        let closed_changed = Arc::new(tokio::sync::Notify::new());
        let observed_closed_changed = closed_changed.clone();
        let wire = Arc::new(Mutex::new(HashMap::new()));
        let observed = wire.clone();
        let sessions = Arc::new(Mutex::new(BTreeSet::new()));
        let ids = sessions.clone();
        let (stop, mut stopping) = tokio::sync::oneshot::channel();
        let (retire, mut retiring) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            let mut originals: Vec<quinn::Connection> = Vec::new();
            loop {
                let raw = tokio::select! {biased;_=&mut stopping=>break,
                    _=retiring.recv()=>{originals.last().unwrap().close(0x100u32.into(),b"actual peer retirement");continue},
                raw=endpoint.accept_connection()=>raw.unwrap()};
                originals.push(raw.clone());
                let count = count.clone();
                let observed = observed.clone();
                let ids = ids.clone();
                let closed = observed_closed.clone();
                let closed_changed = observed_closed_changed.clone();
                clients.spawn(async move {
                    let profile=Hysteria2AuthenticatedInboundProfile::from_options_refs(Hysteria2InboundOptionsRef {users:[Hysteria2InboundUserRef {password:"p",principal_key:None,up_bps:None,down_bps:None,device_limit:None,quota_remaining_bytes:None,policy_revision:None}]});
                    let connection=profile.accept_authenticated_connection(raw.clone()).await.unwrap();count.fetch_add(1,Ordering::SeqCst);
                    let source=InboundDatagramMultiplexer::datagram_source(&connection);
                    let mut streams=tokio::task::JoinSet::new();
                    tokio::select! {
                        _=async {while let Ok(data)=source.read_datagram().await {
                            let packet=::hysteria2::udp::Hysteria2InboundUdpCodec.decode_datagram(&data).unwrap();ids.lock().unwrap().insert(packet.session_id());
                            *observed.lock().unwrap().entry((packet.target().clone(),packet.port())).or_default()+=data.len() as u64;
                            if raw.send_datagram(data).is_err(){break;}
                        }}=>{},
                        _=async {while let Ok(Some((_,mut stream)))=InboundStreamMultiplexer::accept_next_tcp_stream(&connection).await {
                            let response=InboundStreamMultiplexer::response_protocol(&connection);
                            streams.spawn(async move {
                                response.send_ok(&mut stream).await.unwrap();
                                let mut bytes=[0;4];
                                loop {
                                    match stream.read(&mut bytes[..1]).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(1) => {},
                                        Ok(_) => unreachable!(),
                                    }
                                    stream.read_exact(&mut bytes[1..]).await.unwrap();
                                    assert_eq!(&bytes,b"ping");
                                    if stream.write_all(&bytes).await.is_err() {break;}
                                }
                            });
                        }}=>{},
                    }
                    raw.close(0u32.into(),b"test peer complete");while let Some(joined)=streams.join_next().await {joined.unwrap();}
                    closed.fetch_add(1,Ordering::SeqCst);closed_changed.notify_waiters();
                });
            }
            for raw in originals {
                raw.close(0u32.into(), b"fixture shutdown");
            }
            while let Some(joined) = clients.join_next().await {
                joined.unwrap();
            }
        });
        Self {
            port,
            authenticated,
            closed,
            closed_changed,
            wire,
            sessions,
            stop,
            retire,
            task,
            _material: material,
        }
    }
    fn proxy(&self) -> Proxy {
        Proxy::new(zero_config::RuntimeConfig::parse(&serde_json::json!({
            "outbounds":[{"tag":"hy","protocol":{"type":"hysteria2","server":"127.0.0.1","server_name":"localhost","port":self.port,"password":"p","insecure":true}}],
            "route":{"final":{"type":"route","outbound":"hy"}}
        }).to_string()).unwrap()).unwrap()
    }
    async fn wait_closed(&self, count: usize) {
        loop {
            let changed = self.closed_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.closed.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }
    async fn close(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
    async fn retire_current(&self, closed: usize) {
        self.retire.send(()).await.unwrap();
        self.wait_closed(closed).await;
    }
}
#[tokio::test]
async fn standard_hysteria2_proxy_tcp_udp_empty_multitarget_and_packet_paths_share_actual_authentication(
) {
    tokio::time::timeout(Duration::from_secs(20),async {
        let server=Server::new().await;let proxy=server.proxy();
        let config=proxy.engine().runtime_snapshot().config().clone();
        proxy.protocols.start_components(config.clone(),end()).await.unwrap();proxy.protocols.commit_components(&config,||Ok(())).unwrap();
        let mut session=Session::new(0,Address::Ipv4([203,0,113,1]),443,Network::Tcp,ProtocolType::new("test"));
        proxy.engine().prepare_session(&mut session,"incoming").unwrap();
        let (resolved,_)=proxy.engine().resolve_route_decision(RouteDecision::Route("hy".into())).unwrap();
        let established=crate::runtime::tcp_dispatch::dispatch_tcp_outbound(proxy.tcp_runtime_services().for_inbound("incoming".into()),&session,resolved,zero_engine::RouteMode::Flow,crate::runtime::tcp_dispatch::TcpDispatchIntent::Traffic).await.map_err(|failure|failure.error).unwrap();
        let mut stream=established.into_relay_stream().unwrap();stream.write_all(b"ping").await.unwrap();let mut response=[0;4];stream.read_exact(&mut response).await.unwrap();assert_eq!(&response,b"ping");
        let returns=Arc::new(crate::component::datagram_retirement::DatagramReturns::default());
        let runtime=crate::runtime::udp_ingress::UdpIngressRuntime::new(proxy.tcp_runtime_services()).with_datagram_returns(returns.clone());
        let mut dispatch=runtime.new_dispatch("incoming").await.unwrap();
        let mut flow_bytes=HashMap::new();
        for (target,payload) in [(Address::Ipv4([203,0,113,1]),Vec::new()),(Address::Ipv4([203,0,113,1]),b"flow".to_vec()),(Address::Ipv4([203,0,113,2]),vec![7;1600])] {
            let id=dispatch.dispatch(crate::runtime::pipe::UdpPipeInput {peer_identity:None,target:target.clone(),port:53,payload:&payload,route_target:None,sniffed_original_target:None,sniffed_host_source:None,skip_fake_ip_restore:false,protocol:ProtocolType::new("test"),auth:None,source_addr:None,client_session_id:None,transparent_target:false,transparent_original_target:None,transparent_host_source:None}).await.unwrap();
            let (_,tasks,_)=dispatch.poll_sockets();let packet=tasks.join_next_with_id().await.unwrap().unwrap().unwrap();
            assert_eq!((&packet.0,packet.1,&packet.2),(&target,53,&payload));
            crate::runtime::udp_delivery::record_chain_udp_response_parts(&proxy.udp_runtime_services(),&dispatch,packet.0,packet.1,packet.2,packet.3);
            let active=proxy.engine().active_sessions().into_iter().find(|session|session.id==id).unwrap();
            assert_eq!(active.outbound_tag.as_deref(),Some("hy"));
            let expected=flow_bytes.entry(target.clone()).or_insert(0u64);*expected+=payload.len() as u64;
            assert_eq!(active.outbound_tx_bytes,*expected,"HY2 Flow TX preserves payload-only accounting");
            assert!(server.wire.lock().unwrap()[&(target.clone(),53)]>*expected,"real framed bytes remain distinct from payload Flow");
        }
        #[cfg(feature="raw-ip-runtime")]
        {
            let services=proxy.tcp_runtime_services();let plan=services.execution().components.as_ref().unwrap().clone();let slot=plan.configured("hy").unwrap();
            let operation=crate::component::stream_bridge::datagram::packet_path::StandardPacketPath::prepare(&slot,"hy","127.0.0.1",server.port);
            let path=operation.build_carrier(crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(&services.execution())).await.unwrap();
            for (target,payload) in [(Address::Ipv4([198,51,100,1]),b"path".as_slice()),(Address::Ipv4([198,51,100,2]),b"".as_slice())] {
                path.send_to(&target,123,payload).await.unwrap();let mut response=[0;32];let count=path.recv_from(&mut response).await.unwrap();assert_eq!(&response[..count],payload);
            }
            drop(path);drop(operation);assert_eq!(server.sessions.lock().unwrap().len(),3,"two business flows and one multi-target packet association never share response receivers");
        }
        assert_eq!(server.authenticated.load(Ordering::SeqCst),1,"TCP, business UDP and packet carrier use the same real authenticated connection");
        stream.shutdown().await.unwrap();drop(stream);
        let (_,mut sockets,responses)=dispatch.finish_retaining_io();let receipt=responses.shutdown(end()).await.unwrap();assert!(receipt.matches(&responses));assert!(sockets.close_originals().is_complete());
        let retired=proxy.tcp_runtime_services().execution().retired_datagrams();assert!(retired.wait_returns(&returns,end()).await.unwrap().is_complete());
        proxy.protocols.shutdown_components(end()).await.unwrap();server.close().await;
    }).await.unwrap();
}

#[path = "shared/lifecycle.rs"]
mod lifecycle;
