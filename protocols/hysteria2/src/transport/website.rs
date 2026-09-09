//! Website-only HTTP/TLS entry points; no HY2 authentication over TCP.
use super::http3::Masquerade;
use bytes::Bytes;
use std::{io, net::SocketAddr, sync::Arc};
use zero_transport::http_server::{HttpExchange, HttpHandler, RequestContext};

#[derive(Clone)]
pub struct Hysteria2Website {
    masquerade: Masquerade,
    tls: Option<tokio_rustls::TlsAcceptor>,
    quic_port: u16,
    redirect_port: Option<u16>,
}
impl Hysteria2Website {
    pub fn new(masquerade: Masquerade, quic_port: u16, redirect_port: Option<u16>) -> Self {
        Self {
            masquerade,
            tls: None,
            quic_port,
            redirect_port,
        }
    }
    pub fn with_tls(
        mut self,
        plan: &super::Hysteria2InboundBindPlan,
    ) -> Result<Self, zero_transport::RuntimeError> {
        self.tls = Some(plan.website_tls()?);
        self.redirect_port = None;
        Ok(self)
    }
    pub async fn serve<S>(
        &self,
        socket: S,
        peer: SocketAddr,
    ) -> Result<(), zero_transport::RuntimeError>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let context = RequestContext {
            peer,
            tls: self.tls.is_some(),
        };
        let handler = Arc::new(self.clone());
        if let Some(tls) = &self.tls {
            let stream =
                tokio::time::timeout(std::time::Duration::from_secs(10), tls.accept(socket))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "website TLS handshake timed out")
                    })??;
            zero_transport::http_server::serve_connection(stream, context, handler).await?;
        } else {
            zero_transport::http_server::serve_connection(socket, context, handler).await?;
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl HttpHandler for Hysteria2Website {
    async fn serve(
        &self,
        request: http::Request<()>,
        stream: &mut dyn HttpExchange,
    ) -> io::Result<()> {
        if let Some(port) = self.redirect_port {
            let host = request
                .uri()
                .authority()
                .map(|v| v.as_str())
                .or_else(|| request.headers().get("host").and_then(|h| h.to_str().ok()))
                .ok_or_else(|| io::Error::other("missing website host"))?;
            let authority: http::uri::Authority = host.parse().map_err(io::Error::other)?;
            let host = authority.host();
            let authority = if port == 443 {
                host.to_owned()
            } else {
                format!("{host}:{port}")
            };
            let location = format!(
                "https://{authority}{}",
                request
                    .uri()
                    .path_and_query()
                    .map(|v| v.as_str())
                    .unwrap_or("/")
            );
            stream
                .send_response(
                    http::Response::builder()
                        .status(301)
                        .header("location", location)
                        .header("content-length", 0)
                        .body(())
                        .map_err(io::Error::other)?,
                )
                .await?;
            return stream.finish().await;
        }
        let alt_svc = format!("h3=\":{}\"; ma=2592000", self.quic_port)
            .parse()
            .map_err(io::Error::other)?;
        self.masquerade
            .serve(
                request,
                &mut WebsiteExchange {
                    inner: stream,
                    alt_svc,
                },
            )
            .await
    }
}
struct WebsiteExchange<'a> {
    inner: &'a mut dyn HttpExchange,
    alt_svc: http::HeaderValue,
}
#[async_trait::async_trait]
impl HttpExchange for WebsiteExchange<'_> {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        self.inner.recv_data().await
    }
    async fn send_response(&mut self, mut response: http::Response<()>) -> io::Result<()> {
        response
            .headers_mut()
            .insert("alt-svc", self.alt_svc.clone());
        self.inner.send_response(response).await
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.inner.send_data(data).await
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.inner.finish().await
    }
}
