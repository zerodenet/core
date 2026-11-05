mod pool;
use super::*;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
pub(super) struct TlsDns {
    address: std::net::SocketAddr,
    pub(super) connections: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) certificate: rustls::pki_types::CertificateDer<'static>,
    pub(super) requests: mpsc::UnboundedReceiver<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl TlsDns {
    pub(super) async fn new(doh: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let certified = rcgen::generate_simple_self_signed(vec!["native.tls.test".into()]).unwrap();
        let certificate = certified.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key.into())
        .unwrap();
        if doh {
            tls.alpn_protocols = vec![b"h2".to_vec()];
        }
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let (sender, requests) = mpsc::unbounded_channel();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_count = connections.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = tokio::select! {biased;_=cancel.cancelled()=>break,result=listener.accept()=>result.unwrap()};
                accepted_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let acceptor = acceptor.clone();
                let sender = sender.clone();
                let cancel = cancel.clone();
                children.spawn(async move {
                    let accepted = tokio::select! {biased;_=cancel.cancelled()=>return,result=acceptor.accept(socket)=>result};
                    let Ok(mut stream) = accepted else { return };
                    if doh {
                        serve_h2(stream,sender,cancel).await;
                    } else {
                        let work = async {
                            let size=stream.read_u16().await.unwrap() as usize;
                            let mut query=vec![0;size];stream.read_exact(&mut query).await.unwrap();
                            let (reply,answer)=oneshot::channel();sender.send((query,reply)).unwrap();
                            if let Ok(answer)=answer.await {stream.write_u16(answer.len() as u16).await.unwrap();stream.write_all(&answer).await.unwrap();}
                        };
                        tokio::select! {biased;_=cancel.cancelled()=>{},_=work=>{}}
                    }
                });
            }
            while let Some(result) = children.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            connections,
            certificate,
            requests,
            stop,
            task,
        }
    }
    pub(super) fn server(&self, doh: bool) -> zero_config::DnsServerConfig {
        if doh {
            zero_config::DnsServerConfig::Doh {
                host: "127.0.0.1".into(),
                port: self.address.port(),
                path: "/dns-query".into(),
                bootstrap: vec![],
                server_name: Some("native.tls.test".into()),
                detour: None,
            }
        } else {
            zero_config::DnsServerConfig::Dot {
                host: "127.0.0.1".into(),
                port: self.address.port(),
                bootstrap: vec![],
                server_name: Some("native.tls.test".into()),
                detour: None,
            }
        }
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
async fn serve_h2(
    stream: tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    sender: mpsc::UnboundedSender<(Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    cancel: CancellationToken,
) {
    let accepted = tokio::select! {biased;_=cancel.cancelled()=>return,result=h2::server::handshake(stream)=>result};
    let Ok(mut connection) = accepted else { return };
    let mut responses = tokio::task::JoinSet::new();
    let local = cancel.child_token();
    loop {
        tokio::select! {biased;
            _=local.cancelled()=>break,
            joined=responses.join_next(),if !responses.is_empty()=>{joined.unwrap().unwrap();},
            request=connection.accept()=>{
                let Some(Ok((request,mut respond)))=request else {break};
                let sender=sender.clone();let cancel=local.clone();
                responses.spawn(async move {
                    let work=async {
                        assert_eq!(request.uri().path(),"/dns-query");
                        assert_eq!(request.uri().host(),Some("native.tls.test"));
                        let length=request.headers()["content-length"].to_str().unwrap().parse::<usize>().unwrap();
                        let mut body=request.into_body();let mut query=Vec::new();
                        while let Some(chunk)=body.data().await {let chunk=chunk.unwrap();query.extend_from_slice(&chunk);body.flow_control().release_capacity(chunk.len()).unwrap();}
                        assert_eq!(length,query.len());
                        let (reply,answer)=oneshot::channel();sender.send((query,reply)).unwrap();
                        if let Ok(answer)=answer.await {
                            let response=http_uri::Response::builder().status(200).header("content-type","application/dns-message").body(()).unwrap();
                            if let Ok(mut stream)=respond.send_response(response,false) {let _=stream.send_data(bytes::Bytes::from(answer),true);}
                        }
                    };
                    tokio::select! {biased;_=cancel.cancelled()=>{},_=work=>{}}
                });
            }
        }
    }
    local.cancel();
    while let Some(result) = responses.join_next().await {
        result.unwrap();
    }
}
#[tokio::test]
async fn actual_proxy_native_dot_query_uses_original_tcp_controller_and_host_task() {
    exchange(false, false).await;
}
#[tokio::test]
async fn actual_proxy_native_doh_queries_reuse_one_physical_original_and_host_driver() {
    exchange(true, false).await;
}
#[tokio::test]
async fn actual_proxy_native_doh_shutdown_joins_actual_pool_driver_and_confirms_original_close() {
    exchange(true, true).await;
}

#[test]
fn actual_proxy_native_encrypted_dns_runs_on_a_two_mib_worker_stack() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::spawn(async {
            exchange(false, false).await;
            exchange(true, false).await;
            exchange(true, true).await;
        })
        .await
        .unwrap();
    });
}

async fn exchange(doh: bool, shutdown: bool) {
    exchange_in(doh, shutdown, true).await;
}
async fn exchange_in(doh: bool, shutdown: bool, background: bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut tls = TlsDns::new(doh).await;
        let plain = Dns::new().await;
        let mut dns = plain.config();
        dns.servers.insert("native".into(), tls.server(doh));
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns);
        let (proxy, running, _) = if background {
            start(config).await
        } else {
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            let handle = ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            );
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(5))
                .await
                .unwrap();
            (proxy, running, handle)
        };
        if doh {
            zero_dns::test_support::install_doh_ca(
                &proxy.resolver,
                "native",
                tls.certificate.clone(),
            )
            .unwrap();
        } else {
            zero_dns::test_support::install_dot_ca(
                &proxy.resolver,
                "native",
                tls.certificate.clone(),
            )
            .unwrap();
        }
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("encrypted.node.test").await })
        };
        let (query, reply) = tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let suspended = pending(&proxy).await;
        let prior = original(&suspended);
        assert!(prior.closed().is_none());
        if shutdown {
            running.shutdown().await.unwrap();
            assert!(querying.await.unwrap().is_err());
        } else {
            reply
                .send(zero_dns::udp::build_dns_response(
                    &query,
                    &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
                ))
                .unwrap();
            querying.await.unwrap().unwrap();
            if doh {
                assert!(
                    !suspended.task.is_joined(),
                    "the healthy physical driver is reused"
                );
                assert!(prior.closed().is_none());
                let witness =
                    zero_dns::test_support::doh_pool_witness(&proxy.resolver, "native").unwrap();
                assert_eq!(witness.registered_driver_tasks, 1);
                assert_eq!(witness.active_leases, 0);
                let next = tokio::spawn({
                    let resolver = proxy.resolver.clone();
                    async move { resolver.resolve_node("reused.encrypted.node.test").await }
                });
                let (query, reply) =
                    tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
                        .await
                        .unwrap()
                        .unwrap();
                let same = pending(&proxy).await;
                assert!(original(&same).same_original(&prior));
                assert!(same.task.same_registration(&suspended.task));
                assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
                reply
                    .send(zero_dns::udp::build_dns_response(
                        &query,
                        &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
                    ))
                    .unwrap();
                next.await.unwrap().unwrap();
            } else {
                suspended.task.wait().await;
                closed(&prior);
            }
            running.shutdown().await.unwrap();
        }
        assert!(suspended.task.is_joined());
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        tls.close().await;
        plain.close().await;
    })
    .await
    .unwrap();
}
