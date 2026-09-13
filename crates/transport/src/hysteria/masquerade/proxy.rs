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
pub(super) async fn serve(
    client: &crate::http_client::HttpClient,
    base: &url::Url,
    rewrite_host: bool,
    request: http::Request<()>,
    stream: super::super::request::RequestStream,
) -> io::Result<()> {
    let (mut sender, mut receiver) = stream.split();
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
    parts.headers.remove("host");
    if !rewrite_host {
        if let Some(host) = host {
            parts
                .headers
                .insert("host", host.parse().map_err(io::Error::other)?);
        }
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<hyper::body::Frame<Bytes>>>(4);
    let upload = async move {
        while let Some(mut data) = receiver.recv_data().await.map_err(io::Error::other)? {
            while data.has_remaining() {
                let count = data.remaining().min(16384);
                let frame = hyper::body::Frame::data(data.copy_to_bytes(count));
                if tx.send(Ok(frame)).await.is_err() {
                    return Ok::<_, io::Error>(());
                }
            }
        }
        if let Some(mut trailers) = receiver.recv_trailers().await.map_err(io::Error::other)? {
            strip_hop_headers(&mut trailers);
            let _ = tx.send(Ok(hyper::body::Frame::trailers(trailers))).await;
        }
        Ok(())
    };
    let body = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    let request = http::Request::from_parts(parts, body);
    let download = async {
        let response = match client.send_stream(request).await {
            Ok(response) => response,
            Err(_) => {
                sender
                    .send_response(
                        http::Response::builder()
                            .status(502)
                            .header("content-length", "0")
                            .body(())
                            .unwrap(),
                    )
                    .await
                    .map_err(io::Error::other)?;
                return sender.finish().await.map_err(io::Error::other);
            }
        };
        let (mut parts, mut body) = response.into_parts();
        strip_hop_headers(&mut parts.headers);
        sender
            .send_response(http::Response::from_parts(parts, ()))
            .await
            .map_err(io::Error::other)?;
        while let Some(frame) = body.frame().await? {
            match frame.into_data() {
                Ok(data) => sender.send_data(data).await.map_err(io::Error::other)?,
                Err(frame) => {
                    if let Ok(mut trailers) = frame.into_trailers() {
                        strip_hop_headers(&mut trailers);
                        sender
                            .send_trailers(trailers)
                            .await
                            .map_err(io::Error::other)?;
                        return Ok(());
                    }
                }
            }
        }
        sender.finish().await.map_err(io::Error::other)
    };
    tokio::pin!(upload, download);
    tokio::select! {
        result=&mut download=>result,
        result=&mut upload=>{result?;download.await},
    }
}
