//! Ordinary HTTP/HTTPS origin carrier with verified TLS and no redirect handling.
use bytes::Bytes;
use futures_util::Stream;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioExecutor,
};
use std::{io, sync::Arc, time::Duration};

type RequestBody = UnsyncBoxBody<Bytes, io::Error>;
type OriginClient = Client<HttpsConnector<HttpConnector>, RequestBody>;

#[derive(Clone)]
pub struct HttpClient(Origin);
#[derive(Clone)]
enum Origin {
    Network(Arc<OriginClient>),
    #[cfg(unix)]
    Unix(
        Arc<Client<unix::Connector, RequestBody>>,
        Arc<std::path::PathBuf>,
    ),
}
#[cfg(unix)]
mod unix;
impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HttpClient")
    }
}
impl HttpClient {
    pub fn scoped_endpoint(&self, url: &url::Url) -> io::Result<zero_traits::FallbackEndpoint> {
        #[cfg(unix)]
        if let Origin::Unix(_, path) = &self.0 {
            return Ok(zero_traits::FallbackEndpoint::Unix {
                path: path.to_string_lossy().into_owned(),
            });
        }
        Ok(zero_traits::FallbackEndpoint::Tcp {
            server: url
                .host_str()
                .ok_or_else(|| io::Error::other("origin host missing"))?
                .trim_start_matches('[')
                .trim_end_matches(']')
                .into(),
            port: url
                .port_or_known_default()
                .ok_or_else(|| io::Error::other("origin port missing"))?,
        })
    }

    pub fn new() -> Result<Self, io::Error> {
        Self::with_insecure(false)
    }
    pub fn with_insecure(insecure: bool) -> Result<Self, io::Error> {
        let tls = tls_config(insecure)?;
        let mut tcp = HttpConnector::new();
        tcp.enforce_http(false);
        tcp.set_connect_timeout(Some(Duration::from_secs(10)));
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .enable_http2()
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
        let path = Arc::new(path);
        Self(Origin::Unix(
            Arc::new(
                Client::builder(TokioExecutor::new())
                    .pool_idle_timeout(Duration::from_secs(30))
                    .pool_max_idle_per_host(4)
                    .build(unix::Connector(path.clone())),
            ),
            path,
        ))
    }
    pub async fn send(
        &self,
        request: http::Request<Bytes>,
    ) -> Result<http::Response<HttpBody>, io::Error> {
        self.send_body(request.map(|bytes| {
            Full::new(bytes)
                .map_err(|never| match never {})
                .boxed_unsync()
        }))
        .await
    }
    pub async fn send_stream<S>(
        &self,
        request: http::Request<S>,
    ) -> Result<http::Response<HttpBody>, io::Error>
    where
        S: Stream<Item = Result<hyper::body::Frame<Bytes>, io::Error>> + Send + 'static,
    {
        self.send_body(request.map(|stream| StreamBody::new(stream).boxed_unsync()))
            .await
    }
    async fn send_body(
        &self,
        request: http::Request<RequestBody>,
    ) -> Result<http::Response<HttpBody>, io::Error> {
        let response = match &self.0 {
            Origin::Network(client) => client.request(request).await,
            #[cfg(unix)]
            Origin::Unix(client, _) => client.request(request).await,
        };
        response
            .map(|r| r.map(|body| HttpBody { body, stop: None }))
            .map_err(io::Error::other)
    }
}
mod scoped;
fn tls_config(insecure: bool) -> io::Result<rustls::ClientConfig> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
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
    Ok(tls)
}
pub struct HttpBody {
    body: hyper::body::Incoming,
    stop: Option<zero_platform_tokio::contract::CancellationToken>,
}
impl Drop for HttpBody {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.cancel();
        }
    }
}

impl HttpBody {
    pub async fn frame(&mut self) -> Result<Option<hyper::body::Frame<Bytes>>, io::Error> {
        self.body
            .frame()
            .await
            .transpose()
            .map_err(io::Error::other)
    }
    pub async fn data(&mut self) -> Result<Option<Bytes>, io::Error> {
        while let Some(frame) = self.body.frame().await {
            if let Ok(data) = frame.map_err(io::Error::other)?.into_data() {
                return Ok(Some(data));
            }
        }
        Ok(None)
    }
}

#[cfg(all(test, feature = "http_server"))]
#[path = "../tests/http_client/mod.rs"]
mod tests;
