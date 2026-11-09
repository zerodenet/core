//! Explicit independent-library peer: three real wire versions, one registry.
//! It supplies no Kernel grant, Source witness, NativeClosed or Scope ACK.
#[path = "peer/forwarding.rs"]
mod forwarding;
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::CancellationToken;
use zero_transport::{profile::OwnedSplitHttpProfile, split_http::SplitHttpRegistry};
pub(super) struct Peer {
    addresses: [SocketAddr; 3],
    stop: CancellationToken,
    endpoint: quinn::Endpoint,
    joins: Vec<tokio::task::JoinHandle<()>>,
}
impl Peer {
    pub async fn new(certificate: &rcgen::CertifiedKey<rcgen::KeyPair>) -> Self {
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der())
                .into(),
        )
        .unwrap();
        let mut h2 = config.clone();
        h2.alpn_protocols = vec![b"h2".to_vec()];
        let mut h3 = config;
        h3.alpn_protocols = vec![b"h3".to_vec()];
        let endpoint = quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(h3).unwrap(),
            )),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let plain = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secure = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addresses = [
            plain.local_addr().unwrap(),
            secure.local_addr().unwrap(),
            endpoint.local_addr().unwrap(),
        ];
        let registry = SplitHttpRegistry::new();
        let stop = CancellationToken::new();
        let profile = OwnedSplitHttpProfile {
            host: None,
            path: "/original/".into(),
            mode: "auto".into(),
            options: Default::default(),
        };
        let first = listen_tcp(plain, None, registry.clone(), profile.clone(), stop.clone());
        let second = listen_tcp(
            secure,
            Some(tokio_rustls::TlsAcceptor::from(Arc::new(h2))),
            registry.clone(),
            profile.clone(),
            stop.clone(),
        );
        let (third_endpoint, third_stop) = (endpoint.clone(), stop.clone());
        let third = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=third_stop.cancelled()=>break,
                    incoming=third_endpoint.accept()=> {
                        let Some(incoming)=incoming else {break};
                        let (registry,profile,stop)=(registry.clone(),profile.clone(),third_stop.clone());
                        connections.spawn(async move {
                            let connection=incoming.await.unwrap();
                            let source=zero_transport::split_http::accept_xhttp_h3_connection(connection,&profile,&registry);
                            forwarding::serve(source,stop).await;
                        });
                    }
                    joined=connections.join_next(),if !connections.is_empty()=>{joined.unwrap().unwrap();}
                }
            }
            connections.abort_all();
            while let Some(joined) = connections.join_next().await {
                assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
            }
        });
        Self {
            addresses,
            stop,
            endpoint,
            joins: vec![first, second, third],
        }
    }
    pub fn address(&self, version: usize) -> SocketAddr {
        self.addresses[version - 1]
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.endpoint
            .close(0u32.into(), b"explicit library peer stop");
        for joined in self.joins {
            joined.await.unwrap();
        }
        self.endpoint.wait_idle().await;
    }
}
fn listen_tcp(
    listener: TcpListener,
    tls: Option<tokio_rustls::TlsAcceptor>,
    registry: SplitHttpRegistry,
    profile: OwnedSplitHttpProfile,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {biased;
                _=stop.cancelled()=>break,
                accepted=listener.accept()=> {
                    let (io,_)=accepted.unwrap();
                    let (registry,profile,stop,tls)=(registry.clone(),profile.clone(),stop.clone(),tls.clone());
                    connections.spawn(async move {
                        let io=match tls {Some(tls)=>zero_platform_tokio::TcpRelayStream::new(tls.accept(io).await.unwrap()),None=>zero_platform_tokio::TcpRelayStream::new(io)};
                        let source=zero_transport::split_http::accept_xhttp_connection(io,&profile,&registry);
                        forwarding::serve(source,stop).await;
                    });
                }
                joined=connections.join_next(),if !connections.is_empty()=>{joined.unwrap().unwrap();}
            }
        }
        connections.abort_all();
        while let Some(joined) = connections.join_next().await {
            assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
        }
    })
}
