//! Ordinary HTTP/HTTPS origin carrier with verified TLS and no redirect handling.
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{io, sync::Arc, time::Duration};

#[derive(Clone)]
pub struct HttpClient(Client<HttpsConnector<HttpConnector>, Full<Bytes>>);
impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpClient")
    }
}
impl HttpClient {
    pub fn new() -> Result<Self, io::Error> {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(Some(Duration::from_secs(10)));
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        Ok(Self(
            Client::builder(TokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(30))
                .pool_max_idle_per_host(4)
                .build(connector),
        ))
    }
    pub async fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> Result<http::Response<HttpBody>, io::Error> {
        self.0
            .request(request.map(Full::new))
            .await
            .map(|r| r.map(HttpBody))
            .map_err(io::Error::other)
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
