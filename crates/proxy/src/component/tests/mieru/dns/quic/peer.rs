use super::*;
use tokio::sync::{mpsc, oneshot};
pub(super) struct Peer {
    pub address: std::net::SocketAddr,
    pub certificate: rustls::pki_types::CertificateDer<'static>,
    requests: mpsc::UnboundedReceiver<Request>,
    initials: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
    stop: CancellationToken,
    endpoint: quinn::Endpoint,
    task: tokio::task::JoinHandle<()>,
}
pub(super) struct Request {
    pub bytes: Vec<u8>,
    pub client: std::net::SocketAddr,
    reply: oneshot::Sender<Vec<u8>>,
}
impl Request {
    pub fn answer(self, ip: [u8; 4]) {
        self.reply
            .send(zero_dns::udp::build_dns_response(
                &self.bytes,
                &[zero_traits::IpAddress::V4(ip)],
            ))
            .unwrap();
    }
}
impl Peer {
    pub async fn new(hold_handshake: bool) -> Self {
        let certificate =
            rcgen::generate_simple_self_signed(vec!["native.quic.test".into()]).unwrap();
        let cert = certificate.cert.der().clone();
        let key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key.into())
        .unwrap();
        tls.alpn_protocols = vec![b"doq".to_vec()];
        let config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
        ));
        // This endpoint retains the actual bound socket through worker handoff.
        let endpoint = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let address = endpoint.local_addr().unwrap();
        let listening = endpoint.clone();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let (sender, requests) = mpsc::unbounded_channel();
        let (initial, initials) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            loop {
                let incoming = tokio::select! {biased;_=cancel.cancelled()=>break,incoming=listening.accept()=>{let Some(incoming)=incoming else {break};incoming}};
                let sender = sender.clone();
                let cancel = cancel.clone();
                let initial = initial.clone();
                children.spawn(async move {
                    if hold_handshake {let (release,wait)=oneshot::channel();initial.send(release).unwrap();tokio::select!{biased;_=cancel.cancelled()=>return,result=wait=>{if result.is_err(){return;}}}}
                    let accepted=tokio::select!{biased;_=cancel.cancelled()=>return,result=incoming=>result};
                    let Ok(connection)=accepted else {return};
                    let handshake=connection.handshake_data().unwrap().downcast::<quinn::crypto::rustls::HandshakeData>().unwrap();
                    assert_eq!(handshake.protocol.as_deref(),Some(b"doq".as_slice()));
                    assert_eq!(handshake.server_name.as_deref(),Some("native.quic.test"));
                    let accepted=tokio::select!{biased;_=cancel.cancelled()=>return,result=connection.accept_bi()=>result};
                    let Ok((mut send,mut recv))=accepted else{return};
                    let work=async {
                        let size=recv.read_u16().await.unwrap() as usize;assert!(size>=12);
                        let mut query=vec![0;size];recv.read_exact(&mut query).await.unwrap();
                        assert_eq!(recv.read(&mut [0;1]).await.unwrap(),None,"actual DoQ request send half emitted FIN");
                        let (reply,response)=oneshot::channel();sender.send(Request{bytes:query,client:connection.remote_address(),reply}).unwrap();
                        if let Ok(answer)=response.await {
                            send.write_u16(answer.len().try_into().unwrap()).await.unwrap();send.write_all(&answer).await.unwrap();send.finish().unwrap();let _=send.stopped().await;
                        }
                        connection.closed().await;
                    };
                    tokio::select!{biased;_=cancel.cancelled()=>{},_=connection.closed()=>{},_=work=>{}}
                });
            }
            listening.close(0u32.into(), b"fixture shutdown");
            while let Some(result) = children.join_next().await {
                result.unwrap();
            }
            listening.wait_idle().await;
        });
        Self {
            address,
            certificate: cert,
            requests,
            initials,
            stop,
            endpoint,
            task,
        }
    }
    pub fn config(&self) -> zero_config::DnsConfig {
        zero_config::DnsConfig {
            servers: std::collections::BTreeMap::from([(
                "native".into(),
                zero_config::DnsServerConfig::Doq {
                    host: "127.0.0.1".into(),
                    port: self.address.port(),
                    bootstrap: vec![],
                    server_name: Some("native.quic.test".into()),
                    detour: None,
                },
            )]),
            default_server: "native".into(),
            dispatch: vec![],
            reverse_mapping: None,
            answer: zero_config::DnsAnswerConfig::Real,
            cache: None,
            policy: zero_config::DnsPolicyConfig {
                node_server: Some("native".into()),
                address_family: zero_config::DnsAddressFamilyPolicy::Ipv4Only,
                timeout_ms: 5000,
                ..Default::default()
            },
        }
    }
    pub fn trust(&self, proxy: &Proxy) {
        zero_dns::test_support::install_doq_ca(&proxy.resolver, "native", self.certificate.clone())
            .unwrap();
    }
    pub async fn request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(4), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub async fn initial(&mut self) -> oneshot::Sender<()> {
        tokio::time::timeout(Duration::from_secs(4), self.initials.recv())
            .await
            .unwrap()
            .unwrap()
    }
    pub fn no_request(&mut self) {
        assert!(matches!(
            self.requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.endpoint.close(0u32.into(), b"fixture shutdown");
        self.task.await.unwrap();
        self.endpoint.wait_idle().await;
    }
}
