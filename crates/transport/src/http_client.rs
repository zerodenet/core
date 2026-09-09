//! Ordinary HTTP/HTTPS origin carrier with verified TLS and no redirect handling.
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{io, sync::Arc, time::Duration};

type OriginClient = Client<HttpsConnector<HttpConnector>, Full<Bytes>>;

#[derive(Clone)]
pub struct HttpClient(Origin);
#[derive(Clone)]
enum Origin {
    Network(Arc<OriginClient>),
    #[cfg(unix)]
    Unix(Arc<Client<unix::Connector, Full<Bytes>>>),
}
#[cfg(unix)]
mod unix;
impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpClient")
    }
}
impl HttpClient {
    pub fn new() -> Result<Self, io::Error> {
        Self::with_insecure(false)
    }
    pub fn with_insecure(insecure: bool) -> Result<Self, io::Error> {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        if insecure {
            tls.dangerous().set_certificate_verifier(Arc::new(
                crate::certificate_verifier::InsecureServerVerifier {
                    provider: Arc::new(rustls::crypto::ring::default_provider()),
                },
            ));
        }
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(Some(Duration::from_secs(10)));
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        Ok(Self(Origin::Network(Arc::new(
            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(4)
                .build(connector),
        ))))
    }
    #[cfg(unix)]
    pub fn unix(path: std::path::PathBuf) -> Self {
        Self(Origin::Unix(Arc::new(
            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(4)
                .build(unix::Connector(Arc::new(path))),
        )))
    }
    pub async fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> Result<http::Response<HttpBody>, io::Error> {
        let response = match &self.0 {
            Origin::Network(client) => client.request(request.map(Full::new)).await,
            #[cfg(unix)]
            Origin::Unix(client) => client.request(request.map(Full::new)).await,
        };
        response.map(|r| r.map(HttpBody)).map_err(io::Error::other)
    }
}
pub struct HttpBody(hyper::body::Incoming);
impl HttpBody {
    pub async fn data(&mut self) -> Result<Option<Bytes>, io::Error> {
        while let Some(frame) = self.0.frame().await {
            if let Ok(data) = frame.map_err(io::Error::other)?.into_data() {
                return Ok(Some(data));
            }
        }
        Ok(None)
    }
}
