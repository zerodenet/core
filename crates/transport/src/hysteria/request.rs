use super::*;
use bytes::{Buf, Bytes};
use std::sync::atomic::{AtomicBool, Ordering};
pub(super) async fn serve<S>(
    request: http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
    profile: &Profile,
    authenticated: &AtomicBool,
    negotiate: impl Fn(u64),
) -> io::Result<()>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    S::SendStream: Send,
    S::RecvStream: Send,
{
    serve_using(request, stream, profile, authenticated, negotiate, None).await
}
pub(super) async fn serve_scoped<S>(
    request: http::Request<()>,
    stream: h3::server::RequestStream<S, Bytes>,
    profile: &Profile,
    authenticated: &AtomicBool,
    negotiate: impl Fn(u64),
    resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
    caller: &zero_platform_tokio::AcceptedCaller,
) -> io::Result<()>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    S::SendStream: Send,
    S::RecvStream: Send,
{
    serve_using(
        request,
        stream,
        profile,
        authenticated,
        negotiate,
        Some((resources, caller)),
    )
    .await
}
async fn serve_using<S>(
    request: http::Request<()>,
    mut stream: h3::server::RequestStream<S, Bytes>,
    profile: &Profile,
    authenticated: &AtomicBool,
    negotiate: impl Fn(u64),
    scoped: Option<(
        &dyn zero_platform_tokio::contract::component::ResourceAccess,
        &zero_platform_tokio::AcceptedCaller,
    )>,
) -> io::Result<()>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    S::SendStream: Send,
    S::RecvStream: Send,
{
    let auth = request.method() == http::Method::POST
        && request.uri().path() == "/auth"
        && request
            .uri()
            .authority()
            .is_some_and(|a| a.as_str() == "hysteria");
    let allowed = auth
        && (authenticated.load(Ordering::Acquire)
            || request
                .headers()
                .get("hysteria-auth")
                .is_some_and(|value| value.as_bytes() == profile.auth.as_bytes()));
    if !allowed {
        return match scoped {
            Some((resources, caller)) => {
                profile
                    .masquerade
                    .serve_h3_scoped(request, stream, resources, caller)
                    .await
            }
            None => profile.masquerade.serve_h3(request, stream).await,
        };
    }
    if !authenticated.swap(true, Ordering::AcqRel) {
        let peer_downlink = request
            .headers()
            .get("hysteria-cc-rx")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        negotiate(peer_downlink);
    }
    stream
        .send_response(
            http::Response::builder()
                .status(233)
                .header("hysteria-udp", "false")
                .header("hysteria-cc-rx", profile.downlink.to_string())
                .header("hysteria-padding", padding())
                .body(())
                .unwrap(),
        )
        .await
        .map_err(io::Error::other)?;
    stream.finish().await.map_err(io::Error::other)
}
pub(super) struct Exchange<'a, S: h3::quic::BidiStream<Bytes>>(
    pub(super) &'a mut h3::server::RequestStream<S, Bytes>,
);
#[async_trait::async_trait]
impl<S> crate::http_server::HttpExchange for Exchange<'_, S>
where
    S: h3::quic::BidiStream<Bytes> + Send,
    S::SendStream: Send,
    S::RecvStream: Send,
{
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        Ok(self
            .0
            .recv_data()
            .await
            .map_err(io::Error::other)?
            .map(|mut data| {
                let len = data.remaining();
                data.copy_to_bytes(len)
            }))
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.0
            .send_response(response)
            .await
            .map_err(io::Error::other)
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.0.send_data(data).await.map_err(io::Error::other)
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.0.finish().await.map_err(io::Error::other)
    }
}
