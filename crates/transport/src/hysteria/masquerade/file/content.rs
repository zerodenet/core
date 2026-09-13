use super::{
    super::*,
    conditions,
    range::{self, Range},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
#[path = "mime.rs"]
pub(super) mod mime;
pub(super) async fn serve(
    path: &Path,
    request: &http::Request<()>,
    exchange: &mut dyn HttpExchange,
) -> io::Result<()> {
    let head = request.method() == http::Method::HEAD;
    let mut file = tokio::fs::File::open(path).await?;
    let metadata = file.metadata().await?;
    if !metadata.is_file() {
        return error(exchange, 404, head).await;
    }
    let modified = metadata.modified().ok();
    let mut response = http::Response::builder();
    if let Some(modified) = modified {
        response = response.header("last-modified", httpdate::fmt_http_date(modified));
    }
    if let Some(status) = conditions::evaluate(request, modified) {
        exchange
            .send_response(response.status(status).body(()).unwrap())
            .await?;
        return exchange.finish().await;
    }
    let size = metadata.len();
    let ranges = match range::parse(conditions::range(request, modified), size) {
        Ok(ranges) => ranges,
        Err(error) => {
            let mut response = http::Response::builder()
                .status(416)
                .header("content-type", "text/plain; charset=utf-8")
                .header("x-content-type-options", "nosniff");
            if error == range::Error::NoOverlap {
                response = response.header("content-range", format!("bytes */{size}"));
            }
            return respond(
                exchange,
                response.body(()).unwrap(),
                Bytes::from_static(error.message()),
                head,
            )
            .await;
        }
    };
    let mut sample = [0; 512];
    let count = file.read(&mut sample).await?;
    let mime = path
        .extension()
        .and_then(|extension| extension.to_str())
        .and_then(mime::lookup)
        .unwrap_or_else(|| sniff(&sample[..count]));
    response = response.header("accept-ranges", "bytes");
    let boundary: String = rand::random::<[u8; 30]>()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let multipart = ranges.len() > 1;
    let ranges = if ranges.is_empty() {
        vec![Range {
            start: 0,
            length: size,
        }]
    } else {
        response = response.status(206);
        if !multipart {
            response = response.header("content-range", ranges[0].content_range(size));
        }
        ranges
    };
    let footer = format!("\r\n--{boundary}--\r\n");
    let length = if multipart {
        response = response.header(
            "content-type",
            format!("multipart/byteranges; boundary={boundary}"),
        );
        ranges
            .iter()
            .enumerate()
            .map(|(index, range)| {
                range.length + range.prefix(size, mime, &boundary, index == 0).len() as u64
            })
            .sum::<u64>()
            + footer.len() as u64
    } else {
        response = response.header("content-type", mime);
        ranges[0].length
    };
    response = response.header("content-length", length);
    exchange.send_response(response.body(()).unwrap()).await?;
    if !head {
        for (index, range) in ranges.into_iter().enumerate() {
            if multipart {
                exchange
                    .send_data(Bytes::from(range.prefix(size, mime, &boundary, index == 0)))
                    .await?;
            }
            copy(&mut file, range, exchange).await?;
        }
        if multipart {
            exchange.send_data(Bytes::from(footer)).await?;
        }
    }
    exchange.finish().await
}
async fn copy(
    file: &mut tokio::fs::File,
    range: Range,
    exchange: &mut dyn HttpExchange,
) -> io::Result<()> {
    file.seek(std::io::SeekFrom::Start(range.start)).await?;
    let mut remaining = range.length;
    let mut buffer = vec![0; 32768];
    while remaining > 0 {
        let capacity = remaining.min(buffer.len() as u64) as usize;
        let count = file.read(&mut buffer[..capacity]).await?;
        if count == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        exchange
            .send_data(Bytes::copy_from_slice(&buffer[..count]))
            .await?;
        remaining -= count as u64;
    }
    Ok(())
}
