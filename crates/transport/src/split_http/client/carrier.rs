use super::*;
use hyper_util::rt::TokioExecutor;

pub(in crate::split_http) enum Sender {
    Xmux(super::super::xmux::request::Sender),
    Http3(super::super::http3::client::Sender),
    Http1(hyper::client::conn::http1::SendRequest<Body>),
    Http2(hyper::client::conn::http2::SendRequest<Body>),
}
impl Sender {
    pub(in crate::split_http) async fn send_request(
        &mut self,
        mut request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        match self {
            Self::Xmux(sender) => Box::pin(sender.send_request(request)).await,
            Self::Http3(sender) => sender.send_request(request).await,
            Self::Http1(sender) => {
                // The reference disables H1 keepalive for streaming requests.
                // Besides connection ownership, this prevents Go HTTP servers
                // from draining an unfinished request body before flushing the
                // response, which would consume tunneled bytes outside the route.
                if request
                    .extensions()
                    .get::<super::super::request::StreamRequest>()
                    .is_some_and(|kind| kind.0)
                {
                    request
                        .headers_mut()
                        .insert("connection", http::HeaderValue::from_static("close"));
                }
                sender.ready().await.map_err(io::Error::other)?;
                sender
                    .send_request(request)
                    .await
                    .map(|response| response.map(ReceivedBody::Http))
                    .map_err(io::Error::other)
            }
            Self::Http2(sender) => {
                let host = request.headers()["host"]
                    .to_str()
                    .map_err(io::Error::other)?;
                *request.uri_mut() = format!(
                    "{}://{host}{}",
                    request.uri().scheme_str().unwrap_or("http"),
                    request
                        .uri()
                        .path_and_query()
                        .map(|p| p.as_str())
                        .unwrap_or("/")
                )
                .parse()
                .map_err(io::Error::other)?;
                sender
                    .send_request(request)
                    .await
                    .map(|response| response.map(ReceivedBody::Http))
                    .map_err(io::Error::other)
            }
        }
    }
}
pub(super) async fn open_with_settings<S>(
    socket: S,
    h2: bool,
    stream: &mut XhttpStream,
    settings: Option<&[u8]>,
) -> io::Result<Sender>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let state = stream.life.clone();
    if h2 {
        let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
        if let Some(settings) = settings {
            builder.peer_application_settings(settings);
        }
        let (sender, driver) = builder
            .handshake(TokioIo::new(socket))
            .await
            .map_err(io::Error::other)?;
        stream.tasks.push(
            tokio::spawn(async move {
                tokio::select! {
                    result = driver => { if let Err(error) = result { state.fail(error); } }
                    _ = state.cancelled() => {}
                }
            })
            .abort_handle(),
        );
        Ok(Sender::Http2(sender))
    } else {
        let (sender, driver) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
            .await
            .map_err(io::Error::other)?;
        stream.tasks.push(
            tokio::spawn(async move {
                tokio::select! {
                    result = driver => { if let Err(error) = result { state.fail(error); } }
                    _ = state.cancelled() => {}
                }
            })
            .abort_handle(),
        );
        Ok(Sender::Http1(sender))
    }
}
