use super::*;
pub(super) const NAME: &str = "node.vmess.test";
pub(super) struct Endpoint {
    listener: TcpListener,
    acceptor: zero_transport::tls::TlsAcceptor,
    material: tempfile::TempDir,
}
impl Endpoint {
    pub(super) async fn new() -> Self {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec![NAME.into()]).unwrap();
        let path = material.path().join("cert.pem");
        let key = material.path().join("key.pem");
        std::fs::write(&path, cert.cert.pem()).unwrap();
        std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
        let acceptor = zero_transport::tls::build_tls_acceptor(
            &zero_transport::profile::OwnedServerTlsProfile {
                options: Default::default(),
                cert_path: path.display().to_string(),
                key_path: key.display().to_string(),
                alpn: vec!["h2".into()],
                server_fingerprint: None,
            },
            None,
        )
        .unwrap();
        Self {
            listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            acceptor,
            material,
        }
    }
    pub(super) fn config(
        &self,
        dns: &crate::dns_fixture::Dns,
        multi: bool,
    ) -> zero_config::RuntimeConfig {
        zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({
            "runtime":{"dns":dns.config()},
            "outbounds":[{"tag":"node","protocol":{"type":"vmess","server":NAME,"port":self.listener.local_addr().unwrap().port(),"id":ID,"cipher":"aes-128-gcm","grpc":{"service_names":["dns/Tun"],"multi_mode":multi},"tls":{"alpn":["h2"],"ca_cert_path":"cert.pem"}}}],
            "route":{"final":{"type":"route","outbound":"node"}}
        }).to_string(),Some(self.material.path().to_path_buf())).unwrap()
    }
    pub(super) async fn assert_no_connection(&self) {
        use std::{
            future::{poll_fn, Future},
            task::Poll,
        };
        let accept = self.listener.accept();
        tokio::pin!(accept);
        poll_fn(|cx| {
            assert!(
                accept.as_mut().poll(cx).is_pending(),
                "DNS failure must prevent actual dialing"
            );
            Poll::Ready(())
        })
        .await;
    }
    pub(super) fn serve(
        self,
        multi: bool,
        count: usize,
        payloads: Vec<Vec<u8>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _material = self.material;
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..count {
                let (socket, _) = self.listener.accept().await.unwrap();
                let acceptor = self.acceptor.clone();
                let payloads = payloads.clone();
                tasks.spawn(async move {
                    let stream = zero_transport::tls::accept_tls_handshake(
                        &acceptor,
                        zero_platform_tokio::TokioSocket::new(socket),
                    )
                    .await
                    .unwrap();
                    let incoming = zero_transport::grpc::accept_grpc_connection(
                        stream,
                        &zero_transport::profile::OwnedGrpcProfile {
                            service_names: vec!["dns/Tun".into()],
                            multi_mode: multi,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                    let stream = incoming.accept().await.unwrap();
                    echo_io_with_close(
                        zero_platform_tokio::TcpRelayStream::new(stream),
                        "aes-128-gcm".into(),
                        53,
                        payloads,
                        false,
                    )
                    .await;
                    incoming.close();
                });
            }
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
    }
}
pub(super) async fn start_domain(
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
    assert_standard_datagram(&proxy);
    assert_no_legacy_udp(&proxy);
    (proxy, running, handle)
}
pub(super) async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    timeout(Duration::from_secs(3), async {
        loop {
            if let Some(query) = proxy
                .resolver
                .native_query_observations()
                .into_iter()
                .find(|q| q.io_pending && q.original.is_some() && !q.task.is_joined())
            {
                return query;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual native DNS query suspended in I/O")
}
pub(super) fn dns_closed(query: &zero_dns::OwnedDnsQueryObservation) {
    assert!(query.task.is_joined());
    let original = query.original.as_ref().unwrap();
    let closed = original
        .closed()
        .expect("original native DNS close receipt");
    assert!(closed.matches(original) && closed.had_native_acquisition());
}
pub(super) async fn open(
    proxy: Proxy,
    identity: u64,
    payload: &'static [u8],
) -> crate::runtime::udp_dispatch::UdpDispatch {
    let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
        .new_dispatch("incoming")
        .await
        .unwrap();
    let mut packet = input(53, payload);
    packet.client_session_id = Some(identity);
    dispatch.dispatch(packet).await.unwrap();
    let packet = response(&mut dispatch).await;
    assert_eq!(packet.2.as_ref(), payload);
    crate::runtime::udp_delivery::record_chain_udp_response_parts(
        &proxy.udp_runtime_services(),
        &dispatch,
        packet.0,
        packet.1,
        packet.2,
        packet.3,
    );
    dispatch
}
pub(super) async fn retire(proxy: &Proxy, dispatch: crate::runtime::udp_dispatch::UdpDispatch) {
    let completed = dispatch.finish_all();
    assert_eq!(completed[0].upstream.as_ref().unwrap().0, NAME);
    let report = proxy
        .tcp_runtime_services()
        .execution()
        .retired_datagrams()
        .progress_retired(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
}
