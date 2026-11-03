//! One HTTP exchange over the host's original service stream. No native dial,
//! resolver, connection pool or TokioExecutor is reachable through this path.
use super::*;
use zero_platform_tokio::{
    contract::{component::ResourceAccess, scope::TaskClass, stream::BoxStream, CancellationToken},
    AcceptedCaller,
};
#[derive(Clone)]
struct Executor {
    caller: AcceptedCaller,
    stop: CancellationToken,
}
impl<F> hyper::rt::Executor<F> for Executor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        let stop = self.stop.clone();
        if self
            .caller
            .spawn(TaskClass::Auxiliary, move |_| async move {
                tokio::select! {biased;_=stop.cancelled()=>{},_=future=>{}}
                Ok(())
            })
            .is_err()
        {
            self.stop.cancel();
        }
    }
}
impl HttpClient {
    pub async fn send_stream_scoped<S>(
        &self,
        request: http::Request<S>,
        resources: &dyn ResourceAccess,
        caller: &AcceptedCaller,
        insecure: bool,
    ) -> io::Result<http::Response<HttpBody>>
    where
        S: Stream<Item = Result<hyper::body::Frame<Bytes>, io::Error>> + Send + 'static,
    {
        caller.check_open().map_err(io::Error::other)?;
        let uri = request.uri();
        let host = uri
            .host()
            .ok_or_else(|| io::Error::other("missing origin host"))?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let secure = uri.scheme_str() == Some("https");
        let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });
        let endpoint = match &self.0 {
            #[cfg(unix)]
            Origin::Unix(_, path) => zero_traits::FallbackEndpoint::Unix {
                path: path.to_string_lossy().into_owned(),
            },
            _ => zero_traits::FallbackEndpoint::Tcp {
                server: host.clone(),
                port,
            },
        };
        let original = resources
            .open_inbound_stream(caller.clone(), endpoint)
            .await
            .map_err(io::Error::other)?;
        let (io, h2): (BoxStream, bool) = if secure {
            let mut config = tls_config(insecure)?;
            config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let name = rustls::pki_types::ServerName::try_from(host).map_err(io::Error::other)?;
            let stream = connector.connect(name, original).await?;
            let h2 = stream.get_ref().1.alpn_protocol() == Some(b"h2");
            (Box::pin(stream), h2)
        } else {
            (Box::pin(original), false)
        };
        let stop = CancellationToken::new();
        let guard = stop.clone().drop_guard();
        let executor = Executor {
            caller: caller.clone(),
            stop: stop.clone(),
        };
        let mut request = request.map(|stream| StreamBody::new(stream).boxed_unsync());
        if !request.headers().contains_key(http::header::HOST) {
            let authority = request
                .uri()
                .authority()
                .ok_or_else(|| io::Error::other("missing origin authority"))?
                .as_str()
                .to_owned();
            request.headers_mut().insert(
                http::header::HOST,
                authority.parse().map_err(io::Error::other)?,
            );
        }
        let response = if h2 {
            let (mut sender, driver) = hyper::client::conn::http2::Builder::new(executor)
                .handshake(hyper_util::rt::TokioIo::new(io))
                .await
                .map_err(io::Error::other)?;
            let stopped = stop.clone();
            caller.spawn(TaskClass::Auxiliary,move |_|async move {tokio::select! {biased;_=stopped.cancelled()=>{},result=driver=>{if let Err(error)=result {tracing::debug!(%error,"owned HTTP/2 origin ended");}}}Ok(())}).map_err(io::Error::other)?;
            sender
                .send_request(request)
                .await
                .map_err(io::Error::other)?
        } else {
            let (mut sender, driver) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(io))
                    .await
                    .map_err(io::Error::other)?;
            let stopped = stop.clone();
            caller.spawn(TaskClass::Auxiliary,move |_|async move {tokio::select! {biased;_=stopped.cancelled()=>{},result=driver=>{if let Err(error)=result {tracing::debug!(%error,"owned HTTP/1 origin ended");}}}Ok(())}).map_err(io::Error::other)?;
            // Client-connection HTTP/1 sends origin-form, preserving the Host
            // policy selected by the protocol's existing proxy rewrite.
            let (mut parts, body) = request.into_parts();
            parts.uri = parts
                .uri
                .path_and_query()
                .map(|p| p.as_str())
                .unwrap_or("/")
                .parse()
                .map_err(io::Error::other)?;
            sender
                .send_request(http::Request::from_parts(parts, body))
                .await
                .map_err(io::Error::other)?
        };
        guard.disarm();
        Ok(response.map(|body| HttpBody {
            body,
            stop: Some(stop),
        }))
    }
}
