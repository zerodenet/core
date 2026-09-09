//! Protocol-owned HTTP masquerade policy; file IO and origin bodies are streamed.
use bytes::Bytes;
use std::{
    io,
    path::{Path, PathBuf},
};
use zero_transport::http_server::HttpExchange;

#[derive(Debug, Clone, Default)]
pub enum Masquerade {
    #[default]
    NotFound,
    File(PathBuf),
    Proxy {
        url: url::Url,
        rewrite_host: bool,
        x_forwarded: bool,
        client: zero_transport::http_client::HttpClient,
    },
    String {
        content: Bytes,
        status: http::StatusCode,
        content_type: http::HeaderValue,
    },
}
impl Masquerade {
    pub fn file(dir: &str, base: Option<&Path>) -> io::Result<Self> {
        let path = Path::new(dir);
        let path = if path.is_absolute() {
            path.to_owned()
        } else {
            base.unwrap_or(Path::new(".")).join(path)
        };
        let path = path.canonicalize()?;
        if !path.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "masquerade file root is not a directory",
            ));
        }
        Ok(Self::File(path))
    }
    pub fn proxy(url: &str, rewrite_host: bool) -> io::Result<Self> {
        Self::proxy_with_options(url, rewrite_host, false, false)
    }
    pub fn proxy_with_options(
        url: &str,
        rewrite_host: bool,
        insecure: bool,
        x_forwarded: bool,
    ) -> io::Result<Self> {
        let url = crate::settings::validate_proxy_url(url).map_err(io::Error::other)?;
        let (url, client) = if url.scheme() == "unix" {
            #[cfg(unix)]
            {
                let path = crate::settings::decode_unix_origin_path(url.path())
                    .map_err(io::Error::other)?;
                let client = zero_transport::http_client::HttpClient::unix(PathBuf::from(path));
                (url::Url::parse("http://localhost/").unwrap(), client)
            }
            #[cfg(not(unix))]
            {
                return Err(io::Error::other("Unix socket origins are unavailable"));
            }
        } else {
            (
                url,
                zero_transport::http_client::HttpClient::with_insecure(insecure)?,
            )
        };
        Ok(Self::Proxy {
            url,
            rewrite_host,
            x_forwarded,
            client,
        })
    }
    pub fn content(content: &str, status: u16, content_type: &str) -> io::Result<Self> {
        crate::settings::validate_masquerade_response(status, content_type)
            .map_err(io::Error::other)?;
        Ok(Self::String {
            content: Bytes::copy_from_slice(content.as_bytes()),
            status: http::StatusCode::from_u16(status).map_err(io::Error::other)?,
            content_type: content_type.parse().map_err(io::Error::other)?,
        })
    }
    pub(crate) async fn serve(
        &self,
        request: http::Request<()>,
        stream: &mut dyn HttpExchange,
    ) -> Result<(), io::Error> {
        match self {
            Self::NotFound => {
                respond(
                    stream,
                    404,
                    "text/plain",
                    Bytes::from_static(b"404 page not found\n"),
                    request.method() == http::Method::HEAD,
                )
                .await
            }
            Self::String {
                content,
                status,
                content_type,
            } => {
                respond(
                    stream,
                    status.as_u16(),
                    content_type.to_str().map_err(io::Error::other)?,
                    content.clone(),
                    request.method() == http::Method::HEAD,
                )
                .await
            }
            Self::File(root) => serve_file(root, request, stream).await,
            Self::Proxy {
                url,
                rewrite_host,
                x_forwarded,
                client,
            } => proxy(client, url, *rewrite_host, *x_forwarded, request, stream).await,
        }
    }
}
pub(super) async fn respond(
    stream: &mut dyn HttpExchange,
    status: u16,
    content_type: &str,
    body: Bytes,
    head: bool,
) -> io::Result<()> {
    let mut response = http::Response::builder()
        .status(status)
        .header("content-type", content_type);
    if status != 204 {
        response = response.header("content-length", body.len());
    }
    stream
        .send_response(response.body(()).map_err(io::Error::other)?)
        .await
        .map_err(io::Error::other)?;
    if !head && !matches!(status, 204 | 304) && !body.is_empty() {
        stream.send_data(body).await.map_err(io::Error::other)?;
    }
    stream.finish().await.map_err(io::Error::other)
}

mod file;
mod proxy;
use file::serve_file;
use proxy::proxy;
