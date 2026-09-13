use super::*;
use bytes::{Buf, Bytes};
use std::sync::atomic::{AtomicBool, Ordering};
pub(super) type RequestStream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;
pub(super) async fn serve(
    request: http::Request<()>,
    mut stream: RequestStream,
    profile: &Profile,
    authenticated: &AtomicBool,
    connection: &quinn::Connection,
) -> io::Result<()> {
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
        return profile.masquerade.serve_h3(request, stream).await;
    }
    if !authenticated.swap(true, Ordering::AcqRel) {
        let peer_downlink = request
            .headers()
            .get("hysteria-cc-rx")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        profile.negotiate(connection, peer_downlink);
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
pub(super) struct Exchange<'a>(pub(super) &'a mut RequestStream);
#[async_trait::async_trait]
impl crate::http_server::HttpExchange for Exchange<'_> {
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
