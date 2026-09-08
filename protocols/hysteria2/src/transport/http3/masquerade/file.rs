use super::{respond, RequestStream};
use bytes::Bytes;
use std::{io, path::Path};

pub(super) async fn serve_file(
    root: &Path,
    request: http::Request<()>,
    stream: &mut RequestStream,
) -> io::Result<()> {
    let head = request.method() == http::Method::HEAD;
    if request.method() != http::Method::GET && !head {
        return respond(stream, 405, "text/plain", Bytes::new(), false).await;
    }
    let path = match crate::settings::decode_site_path(request.uri().path()) {
        Ok(path) => path,
        Err(_) => return respond(stream, 404, "text/plain", Bytes::new(), head).await,
    };
    let mut candidate = root.join(path);
    if tokio::fs::metadata(&candidate)
        .await
        .is_ok_and(|m| m.is_dir())
    {
        candidate = candidate.join("index.html");
    }
    let candidate = match tokio::fs::canonicalize(candidate).await {
        Ok(path) if path.starts_with(root) => path,
        _ => {
            return respond(
                stream,
                404,
                "text/plain",
                Bytes::from_static(b"404 page not found\n"),
                head,
            )
            .await
        }
    };
    let mut file = match tokio::fs::File::open(&candidate).await {
        Ok(file) => file,
        Err(_) => return respond(stream, 404, "text/plain", Bytes::new(), head).await,
    };
    let metadata = file.metadata().await?;
    if !metadata.is_file() {
        return respond(stream, 404, "text/plain", Bytes::new(), head).await;
    }
    let mime = match candidate.extension().and_then(|v| v.to_str()) {
        Some("html" | "htm") => "text/html; charset=utf-8",
        Some("css") => "text/css",
        Some("js") => "text/javascript",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        _ => "application/octet-stream",
    };
    stream
        .send_response(
            http::Response::builder()
                .status(200)
                .header("content-type", mime)
                .header("content-length", metadata.len())
                .body(())
                .map_err(io::Error::other)?,
        )
        .await
        .map_err(io::Error::other)?;
    if !head {
        let mut buffer = vec![0u8; 32 * 1024];
        loop {
            let size = tokio::io::AsyncReadExt::read(&mut file, &mut buffer).await?;
            if size == 0 {
                break;
            }
            stream
                .send_data(Bytes::copy_from_slice(&buffer[..size]))
                .await
                .map_err(io::Error::other)?;
        }
    }
    stream.finish().await.map_err(io::Error::other)
}
