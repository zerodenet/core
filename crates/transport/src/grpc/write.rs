use super::*;
pub(super) fn spawn_grpc_write_relay(
    mut sender: h2::SendStream<Bytes>,
    mut receiver: mpsc::Receiver<Vec<u8>>,
    progress: std::sync::Arc<super::progress::Progress>,
    server: bool,
) {
    tokio::spawn(async move {
        let result = async {
            while let Some(data) = receiver.recv().await {
                let hunk = encode_grpc_hunk(&data);
                let mut frame = Vec::with_capacity(GRPC_HEADER_LEN + hunk.len());
                frame.extend_from_slice(&grpc_frame_header(hunk.len()));
                frame.extend_from_slice(&hunk);
                write_h2_data(&mut sender, Bytes::from(frame)).await?;
                progress.commit(data.len());
            }
            if server {
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                sender.send_trailers(trailers).map_err(io::Error::other)?;
            } else {
                if let Err(error) = sender.send_data(Bytes::new(), true) {
                    // grpc-go may have already completed/reset the stream after
                    // returning its response. All queued DATA was committed above.
                    if error.is_io() || error.reason().is_some() {
                        return Err(io::Error::other(error));
                    }
                }
            }
            Ok::<_, io::Error>(())
        }
        .await;
        if let Err(error) = result {
            progress.fail(error);
        }
        progress.finish();
    });
}

async fn write_h2_data(sender: &mut h2::SendStream<Bytes>, mut bytes: Bytes) -> io::Result<()> {
    while !bytes.is_empty() {
        sender.reserve_capacity(bytes.len());
        let capacity = futures_util::future::poll_fn(|cx| sender.poll_capacity(cx))
            .await
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?
            .map_err(io::Error::other)?;
        if capacity == 0 {
            continue;
        }
        let chunk = bytes.split_to(bytes.len().min(capacity));
        sender.send_data(chunk, false).map_err(io::Error::other)?;
    }
    Ok(())
}
