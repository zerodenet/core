use super::*;
pub async fn connect_xhttp_stream_one<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<crate::h2::H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let profile = super::super::request::Profile::new(config);
    let request = profile
        .request(
            &profile.options.uplink_http_method,
            "",
            None,
            super::super::body::Body::empty(),
        )?
        .map(|_| ());
    crate::h2::connect_h2_request(stream, request).await
}

pub async fn connect_xhttp_stream_one_http1<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<XhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let profile = super::super::request::Profile::new(config);
    let request = profile.request(
        &profile.options.uplink_http_method,
        "",
        None,
        super::super::body::Body::empty(),
    )?;
    let mut wire = format!("{} {} HTTP/1.1\r\n", request.method(), request.uri());
    for (name, value) in request.headers() {
        wire.push_str(&format!(
            "{}: {}\r\n",
            name,
            value.to_str().map_err(io::Error::other)?
        ));
    }
    wire.push_str("Transfer-Encoding: chunked\r\n\r\n");
    let request = wire;
    let mut stream = stream;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(RuntimeError::Io)?;
    stream.flush().await.map_err(RuntimeError::Io)?;

    Ok(XhttpStreamOne {
        inner: stream,
        decoder: ChunkedDecoder::new(),
        response_headers: Some(Vec::with_capacity(1024)),
        write_finished: false,
    })
}

pub async fn accept_xhttp_stream_one<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<AcceptedXhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let registry = super::super::SplitHttpRegistry::new();
    let incoming = super::super::server::accept_xhttp_connection(stream, config, &registry);
    let inner = incoming
        .first()
        .await
        .ok_or_else(|| io::Error::other("xhttp closed before stream-one request"))?;
    Ok(AcceptedXhttpStreamOne {
        inner,
        marker: core::marker::PhantomData,
    })
}

pub async fn accept_xhttp_stream_one_http1<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<XhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let mut stream = stream;
    let profile = super::super::request::Profile::new(config);
    let mut buf = vec![0u8; profile.options.server_max_header_bytes.max(8192) as usize];
    let mut total = 0;
    let head_end = loop {
        let n = stream
            .read(&mut buf[total..])
            .await
            .map_err(RuntimeError::Io)?;
        if n == 0 {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "xhttp stream-one accept: unexpected EOF before request headers",
            )));
        }
        total += n;
        if let Some(end) = find_header_end(&buf[..total]) {
            break end;
        }
        if total >= buf.len() {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "xhttp stream-one accept: request headers too large",
            )));
        }
    };

    let request = validate_stream_one_http1_request(&buf[..head_end], &profile.path)?;
    profile.validate_padding(&request)?;
    if profile.check_host
        && request.headers().get("host").and_then(|h| h.to_str().ok())
            != Some(profile.host.as_str())
    {
        return Err(io::Error::other("xhttp host mismatch").into());
    }
    let headers = profile.response_headers(&request)?;
    let mut response =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nCache-Control: no-store\r\n".to_owned();
    if !profile.options.no_sse_header {
        response.push_str("Content-Type: text/event-stream\r\n");
    }
    for (name, value) in &headers {
        response.push_str(&format!(
            "{}: {}\r\n",
            name,
            value.to_str().map_err(io::Error::other)?
        ));
    }
    response.push_str("\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await.map_err(RuntimeError::Io)?;

    let prefetched = buf[head_end..total].to_vec();
    Ok(XhttpStreamOne {
        inner: stream,
        decoder: ChunkedDecoder::with_prefetched(prefetched),
        response_headers: None,
        write_finished: false,
    })
}

fn validate_stream_one_http1_request(
    headers: &[u8],
    expected_path: &str,
) -> Result<http::Request<()>, RuntimeError> {
    let headers = std::str::from_utf8(headers).map_err(|_| {
        RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: non-UTF-8 request headers",
        ))
    })?;
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut chunked = false;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let uri: http::Uri = parts
        .next()
        .unwrap_or("")
        .parse()
        .map_err(io::Error::other)?;
    if uri.path() != expected_path {
        return Err(io::Error::other("xhttp stream-one path mismatch").into());
    }
    let mut request = http::Request::builder().method(method).uri(uri);
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        {
            chunked = true;
        }
        request = request.header(name, value.trim());
    }
    if !chunked {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: request must use chunked transfer encoding",
        )));
    }
    request.body(()).map_err(|e| io::Error::other(e).into())
}
