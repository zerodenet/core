use super::{respond, RequestStream};
use bytes::{Buf, Bytes};
use std::io;

fn strip_hop_headers(headers: &mut http::HeaderMap) {
    let tokens: Vec<_> = headers
        .get_all("connection")
        .iter()
        .filter_map(|h| h.to_str().ok())
        .flat_map(|s| s.split(',').map(|v| v.trim().to_owned()))
        .collect();
    for token in tokens {
        headers.remove(token);
    }
    for name in [
        "connection",
        "proxy-connection",
        "keep-alive",
        "transfer-encoding",
        "upgrade",
        "te",
        "trailer",
        "proxy-authorization",
        "proxy-authenticate",
    ] {
        headers.remove(name);
    }
}
pub(super) async fn proxy(
    client: &zero_transport::http_client::HttpClient,
    base: &url::Url,
    rewrite_host: bool,
    request: http::Request<()>,
    stream: &mut RequestStream,
) -> io::Result<()> {
    let mut body = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await.map_err(io::Error::other)? {
        if body.len() + chunk.remaining() > 1024 * 1024 {
            return respond(stream, 413, "text/plain", Bytes::new(), false).await;
        }
        let length = chunk.remaining();
        body.extend_from_slice(&chunk.copy_to_bytes(length));
    }
    let mut url = base.clone();
    url.set_path(&format!(
        "{}/{}",
        base.path().trim_end_matches('/'),
        request.uri().path().trim_start_matches('/')
    ));
    let query = match (base.query(), request.uri().query()) {
        (Some(base), Some(request)) => Some(format!("{base}&{request}")),
        (base, request) => base.or(request).map(str::to_owned),
    };
    url.set_query(query.as_deref());
    let (mut parts, ()) = request.into_parts();
    let host = parts.uri.authority().map(|v| v.as_str().to_owned());
    parts.uri = url.as_str().parse().map_err(io::Error::other)?;
    parts.version = http::Version::HTTP_11;
    strip_hop_headers(&mut parts.headers);
    parts.headers.remove("host");
    parts.headers.remove("content-length");
    if !rewrite_host {
        if let Some(host) = host {
            parts
                .headers
                .insert("host", host.parse().map_err(io::Error::other)?);
        }
    }
    let request = http::Request::from_parts(parts, Bytes::from(body));
    let response = match client.send(request).await {
        Ok(response) => response,
        Err(_) => {
            return respond(
                stream,
                502,
                "text/plain",
                Bytes::from_static(b"Bad Gateway\n"),
                false,
            )
            .await
        }
    };
    let (mut parts, mut body) = response.into_parts();
    strip_hop_headers(&mut parts.headers);
    parts.version = http::Version::HTTP_3;
    stream
        .send_response(http::Response::from_parts(parts, ()))
        .await
        .map_err(io::Error::other)?;
    while let Some(data) = body.data().await? {
        stream.send_data(data).await.map_err(io::Error::other)?;
    }
    stream.finish().await.map_err(io::Error::other)
}
