use super::*;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use zero_platform_tokio::contract::{scope::WeakTaskRegistrar, stream::BoxStream};
#[derive(Clone)]
struct Executor {
    stop: CancellationToken,
    tasks: WeakTaskRegistrar,
    failed: Arc<Mutex<Option<ChannelError>>>,
}
impl<F> hyper::rt::Executor<F> for Executor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, work: F) {
        let stop = self.stop.clone();
        if let Err(source) = self.tasks.spawn(TaskClass::Auxiliary, async move {
            tokio::select! {biased;_=stop.cancelled()=>{},_=work=>{}}
            Ok(())
        }) {
            *self.failed.lock().unwrap() = Some(
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                    .with_source(source),
            );
        }
    }
}
pub(in crate::tls) async fn send(
    resources: &dyn ResourceAccess,
    driver: &TaskJoinWitness,
    tasks: &WeakTaskRegistrar,
    mut request: http::Request<Bytes>,
    limit: usize,
) -> io::Result<http::Response<Vec<u8>>> {
    let stop = tasks
        .cancellation()
        .ok_or_else(|| io::Error::other("original OCSP Source Scope expired"))?;
    let _returned = stop.clone().drop_guard();
    let host = request
        .uri()
        .host()
        .ok_or_else(|| io::Error::other("OCSP host missing"))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let secure = request.uri().scheme_str() == Some("https");
    let port = request
        .uri()
        .port_u16()
        .unwrap_or(if secure { 443 } else { 80 });
    let raw = resources
        .open_component_stream(
            driver,
            tasks,
            zero_traits::FallbackEndpoint::Tcp {
                server: host.clone(),
                port,
            },
        )
        .await
        .map_err(io::Error::other)?;
    let (stream, h2): (BoxStream, bool) = if secure {
        let mut config = crate::http_client::tls_config(false)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let name = rustls::pki_types::ServerName::try_from(host).map_err(io::Error::other)?;
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, raw)
            .await?;
        let h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2");
        (Box::pin(tls), h2)
    } else {
        (Box::pin(raw), false)
    };
    if !request.headers().contains_key(http::header::HOST) {
        let host = request
            .uri()
            .authority()
            .ok_or_else(|| io::Error::other("OCSP authority missing"))?
            .as_str()
            .to_string();
        request
            .headers_mut()
            .insert(http::header::HOST, host.parse().map_err(io::Error::other)?);
    }
    let failed = Arc::new(Mutex::new(None));
    let (driver, exchange): (
        zero_platform_tokio::contract::BoxFuture<'_, io::Result<()>>,
        zero_platform_tokio::contract::BoxFuture<
            '_,
            io::Result<http::Response<hyper::body::Incoming>>,
        >,
    ) = if h2 {
        let (mut sender, connection) = hyper::client::conn::http2::Builder::new(Executor {
            stop: stop.clone(),
            tasks: tasks.clone(),
            failed: failed.clone(),
        })
        .handshake(hyper_util::rt::TokioIo::new(stream))
        .await
        .map_err(io::Error::other)?;
        (
            Box::pin(async move { connection.await.map_err(io::Error::other) }),
            Box::pin(async move {
                sender
                    .send_request(request.map(Full::new))
                    .await
                    .map_err(io::Error::other)
            }),
        )
    } else {
        let uri = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
            .parse()
            .map_err(io::Error::other)?;
        *request.uri_mut() = uri;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(io::Error::other)?;
        (
            Box::pin(async move { connection.await.map_err(io::Error::other) }),
            Box::pin(async move {
                sender
                    .send_request(request.map(Full::new))
                    .await
                    .map_err(io::Error::other)
            }),
        )
    };
    let exchange = async move {
        let response = exchange.await?;
        let (parts, mut body) = response.into_parts();
        let mut bytes = Vec::new();
        if !matches!(parts.status.as_u16(), 301..=303 | 307 | 308) && parts.status.is_success() {
            while let Some(frame) = body.frame().await {
                if let Ok(chunk) = frame.map_err(io::Error::other)?.into_data() {
                    if chunk.len() > limit.saturating_sub(bytes.len()) {
                        return Err(io::Error::other("OCSP HTTP response exceeds size limit"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
            }
        }
        Ok::<_, io::Error>(http::Response::from_parts(parts, bytes))
    };
    tokio::pin!(driver, exchange);
    let result = tokio::select! {biased;result=&mut exchange=>result,result=&mut driver=>{result?;exchange.await}};
    if let Some(source) = failed.lock().unwrap().take() {
        return Err(io::Error::other(source));
    }
    result
}
