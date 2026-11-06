//! External real TLS/gRPC wire peer; its native socket is not a core receipt.
use super::*;
pub(super) struct Directory(pub(super) PathBuf);
impl Directory {
    pub(super) fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "zero-vless-owned-ca-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
pub(super) struct Peer {
    pub(super) address: std::net::SocketAddr,
    pub(super) pem: String,
    pub(super) replacement_pem: String,
    rotate: tokio::sync::watch::Sender<Arc<rustls::ServerConfig>>,
    replacement: Arc<rustls::ServerConfig>,
    pub(super) accepts: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub(super) async fn new() -> Self {
        fn authority() -> (String, Arc<rustls::ServerConfig>) {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let pem = cert.cert.pem();
            let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der())
                    .into(),
            )
            .unwrap();
            tls.alpn_protocols = vec![b"h2".to_vec()];
            (pem, Arc::new(tls))
        }
        let (pem, tls) = authority();
        let (replacement_pem, replacement) = authority();
        let (rotate, current) = tokio::sync::watch::channel(tls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let accepted = accepts.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let socket = tokio::select! {biased;_=cancel.cancelled()=>break,socket=listener.accept()=>socket.unwrap().0};
                accepted.fetch_add(1, Ordering::SeqCst);
                let acceptor = tokio_rustls::TlsAcceptor::from(current.borrow().clone());
                let cancel = cancel.clone();
                connections.spawn(async move {
                let socket=acceptor.accept(socket).await.unwrap();assert_eq!(socket.get_ref().1.alpn_protocol(),Some(b"h2".as_slice()));let incoming=zero_transport::grpc::accept_grpc_connection(socket,&zero_transport::profile::OwnedGrpcProfile{service_names:vec!["actual.ca/Tun".into(),"actual.replacement/Tun".into()],..Default::default()}).unwrap();
                let mut streams=tokio::task::JoinSet::new();loop {
                    let stream=tokio::select!{biased;_=cancel.cancelled()=>break,stream=incoming.accept()=>stream};let Some(mut stream)=stream else{break};
                    let cancel=cancel.clone();streams.spawn(async move {tokio::select!{biased;_=cancel.cancelled()=>{},_=async {
                        let mut expected=vec![0,0x11,0x11,0x11,0x11,0x22,0x22,0x33,0x33,0x44,0x44,0x55,0x55,0x55,0x55,0x55,0x55,0,1,1,187,2,12];expected.extend_from_slice(b"real.example");let mut header=vec![0;expected.len()];stream.read_exact(&mut header).await.unwrap();assert_eq!(header,expected);stream.write_all(&[0,0]).await.unwrap();stream.flush().await.unwrap();let mut bytes=[0;4096];while let Ok(size)=stream.read(&mut bytes).await {if size==0{break;}stream.write_all(&bytes[..size]).await.unwrap();stream.flush().await.unwrap();}
                    }=>{}}});
                }incoming.close();while let Some(result)=streams.join_next().await{result.unwrap();}
            });
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            pem,
            replacement_pem,
            replacement,
            rotate,
            accepts,
            stop,
            task,
        }
    }
    pub(super) fn rotate_authority(&self) {
        drop(self.rotate.send_replace(self.replacement.clone()));
    }
    pub(super) async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
