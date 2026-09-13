//! HTTP appearance of the pinned Xray Hysteria carrier.
use crate::http_server::HttpExchange;
use bytes::Bytes;
use std::{
    io,
    path::{Path, PathBuf},
};
mod file;
mod proxy;
mod sniff;
use sniff::sniff;
#[derive(Debug, Clone, Default)]
pub enum Appearance {
    #[default]
    NotFound,
    String {
        body: Bytes,
        status: http::StatusCode,
        headers: http::HeaderMap,
    },
    File(PathBuf),
}
#[derive(Debug, Clone)]
pub enum Masquerade {
    Appearance(Appearance),
    Proxy {
        url: url::Url,
        rewrite_host: bool,
        client: crate::http_client::HttpClient,
    },
}
impl Masquerade {
    pub fn content(
        content: &str,
        status: u16,
        headers: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
    ) -> io::Result<Self> {
        let status = http::StatusCode::from_u16(if status == 0 { 200 } else { status })
            .map_err(io::Error::other)?;
        if status.is_informational() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "masquerade requires a final status",
            ));
        }
        let mut parsed = http::HeaderMap::new();
        for (key, value) in headers {
            parsed.insert(
                http::header::HeaderName::from_bytes(key.as_ref().as_bytes())
                    .map_err(io::Error::other)?,
                http::HeaderValue::from_str(value.as_ref()).map_err(io::Error::other)?,
            );
        }
        Ok(Self::Appearance(Appearance::String {
            body: Bytes::copy_from_slice(content.as_bytes()),
            status,
            headers: parsed,
        }))
    }
    pub fn file(dir: &str, base: Option<&Path>) -> io::Result<Self> {
        let path = base.unwrap_or(Path::new(".")).join(dir).canonicalize()?;
        if !path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "masquerade file root is not a directory",
            ));
        }
        file::initialize_mime();
        Ok(Self::Appearance(Appearance::File(path)))
    }
    pub fn proxy(url: &str, rewrite_host: bool, insecure: bool) -> io::Result<Self> {
        let url = url::Url::parse(url).map_err(io::Error::other)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host().is_none()
            || url.fragment().is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "masquerade origin must be an HTTP(S) URL",
            ));
        }
        Ok(Self::Proxy {
            url,
            rewrite_host,
            client: crate::http_client::HttpClient::with_insecure(insecure)?,
        })
    }
    pub(super) async fn serve_h3(
        &self,
        request: http::Request<()>,
        mut stream: super::request::RequestStream,
    ) -> io::Result<()> {
        match self {
            Self::Appearance(site) => {
                site.serve(request, &mut super::request::Exchange(&mut stream))
                    .await
            }
            Self::Proxy {
                url,
                rewrite_host,
                client,
            } => proxy::serve(client, url, *rewrite_host, request, stream).await,
        }
    }
}
impl Default for Masquerade {
    fn default() -> Self {
        Self::Appearance(Appearance::default())
    }
}
impl Appearance {
    pub(super) async fn serve(
        &self,
        request: http::Request<()>,
        exchange: &mut dyn HttpExchange,
    ) -> io::Result<()> {
        let head = request.method() == http::Method::HEAD;
        match self {
            Self::NotFound => error(exchange, 404, head).await,
            Self::String {
                body,
                status,
                headers,
            } => {
                let mut response = http::Response::new(());
                *response.status_mut() = *status;
                *response.headers_mut() = headers.clone();
                respond(exchange, response, body.clone(), head).await
            }
            Self::File(root) => file::serve(root, request, exchange).await,
        }
    }
}
async fn respond(
    exchange: &mut dyn HttpExchange,
    mut response: http::Response<()>,
    body: Bytes,
    head: bool,
) -> io::Result<()> {
    let status = response.status();
    let has_body = !matches!(status.as_u16(), 204 | 304);
    if has_body {
        response
            .headers_mut()
            .entry("content-type")
            .or_insert(http::HeaderValue::from_static(sniff(&body)));
        response
            .headers_mut()
            .entry("content-length")
            .or_insert(body.len().to_string().parse().unwrap());
    }
    exchange.send_response(response).await?;
    if has_body && !head && !body.is_empty() {
        exchange.send_data(body).await?;
    }
    exchange.finish().await
}
async fn error(exchange: &mut dyn HttpExchange, status: u16, head: bool) -> io::Result<()> {
    let body = match status {
        404 => "404 page not found\n",
        403 => "403 Forbidden\n",
        405 => "405 Method Not Allowed\n",
        416 => "invalid range: failed to overlap\n",
        _ => "",
    };
    let response = http::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("x-content-type-options", "nosniff")
        .body(())
        .unwrap();
    respond(
        exchange,
        response,
        Bytes::from_static(body.as_bytes()),
        head,
    )
    .await
}
