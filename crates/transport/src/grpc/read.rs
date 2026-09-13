use super::*;
pub(super) async fn read_grpc_hunks(
    recv_stream: h2::RecvStream,
    read_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    multi: bool,
    require_status: bool,
) {
    if let Err(error) = receive(recv_stream, &read_tx, multi, require_status).await {
        let _ = read_tx.send(Err(error)).await;
    }
}

async fn receive(
    mut recv_stream: h2::RecvStream,
    read_tx: &mpsc::Sender<io::Result<Vec<u8>>>,
    multi: bool,
    require_status: bool,
) -> io::Result<()> {
    let mut frame_buf = bytes::BytesMut::new();
    let mut expecting_payload = None;
    loop {
        let data = tokio::select! { _ = read_tx.closed() => return Ok(()), data = recv_stream.data() => data };
        let Some(data) = data else {
            break;
        };
        let data = data.map_err(io::Error::other)?;
        frame_buf.extend_from_slice(&data);
        recv_stream
            .flow_control()
            .release_capacity(data.len())
            .map_err(io::Error::other)?;
        loop {
            if let Some(length) = expecting_payload {
                if frame_buf.len() < length {
                    break;
                }
                let payload = decode_grpc_hunk(&frame_buf.split_to(length), multi)?;
                expecting_payload = None;
                for chunk in payload.chunks(GRPC_MAX_PAYLOAD) {
                    if read_tx.send(Ok(chunk.to_vec())).await.is_err() {
                        return Ok(());
                    }
                }
            } else {
                if frame_buf.len() < GRPC_HEADER_LEN {
                    break;
                }
                let header = frame_buf.split_to(GRPC_HEADER_LEN);
                let (compressed, length) =
                    parse_grpc_frame_header(header.as_ref().try_into().unwrap());
                if compressed || length > 4 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unsupported gRPC compression or message size",
                    ));
                }
                expecting_payload = Some(length);
            }
        }
    }
    if expecting_payload.is_some() || !frame_buf.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated gRPC message",
        ));
    }
    let trailers = tokio::select! { _ = read_tx.closed() => return Ok(()), trailers = recv_stream.trailers() => trailers.map_err(io::Error::other)? };
    if require_status
        || trailers
            .as_ref()
            .is_some_and(|headers| headers.contains_key("grpc-status"))
    {
        status(trailers.as_ref().unwrap_or(&http::HeaderMap::new()))?;
    }
    Ok(())
}

pub(super) fn status(headers: &http::HeaderMap) -> io::Result<()> {
    let status = headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "gRPC response omitted grpc-status",
            )
        })?;
    if status == "0" {
        return Ok(());
    }
    let message = headers
        .get("grpc-message")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let message = percent_encoding::percent_decode_str(message).decode_utf8_lossy();
    Err(io::Error::other(format!("gRPC status {status}: {message}")))
}
