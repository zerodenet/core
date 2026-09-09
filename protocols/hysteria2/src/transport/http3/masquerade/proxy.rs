use super::respond;
use bytes::{Buf, Bytes};
use std::io;
use zero_transport::http_server::HttpExchange;

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
    x_forwarded: bool,
    request: http::Request<()>,
    stream: &mut dyn HttpExchange,
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
    let host = parts
        .uri
        .authority()
        .map(|v| v.as_str().to_owned())
        .or_else(|| {
            parts
                .headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        });
    let context = parts
        .extensions
        .get::<zero_transport::http_server::RequestContext>()
        .copied();
    parts.uri = url.as_str().parse().map_err(io::Error::other)?;
    parts.version = http::Version::HTTP_11;
    strip_hop_headers(&mut parts.headers);
    for name in [
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
    ] {
        parts.headers.remove(name);
    }
    if x_forwarded {
        if let Some(context) = context {
            parts.headers.insert(
                "x-forwarded-for",
                context
                    .peer
                    .ip()
                    .to_string()
                    .parse()
                    .map_err(io::Error::other)?,
            );
            parts.headers.insert(
                "x-forwarded-proto",
                if context.tls { "https" } else { "http" }.parse().unwrap(),
            );
        }
        if let Some(host) = &host {
            parts
                .headers
                .insert("x-forwarded-host", host.parse().map_err(io::Error::other)?);
        }
    }
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

    stream
        .send_response(http::Response::from_parts(parts, ()))
        .await
        .map_err(io::Error::other)?;
    while let Some(data) = body.data().await? {
        stream.send_data(data).await.map_err(io::Error::other)?;
    }
    stream.finish().await.map_err(io::Error::other)
}
