//! Preparation returns the original stream workers without choosing an executor.
use super::*;
use std::future::Future;
pub(super) struct PreparedStream {
    pub(super) stream: GrpcStream,
    pub(super) read: Pin<Box<dyn Future<Output = ()> + Send>>,
    pub(super) write: Pin<Box<dyn Future<Output = ()> + Send>>,
}
pub(super) fn prepare_stream(
    sender: h2::SendStream<Bytes>,
    response: h2::client::ResponseFuture,
    multi: bool,
) -> PreparedStream {
    let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(16);
    let (read_tx, read_rx) = mpsc::channel::<io::Result<Vec<u8>>>(64);
    let progress = std::sync::Arc::new(progress::Progress::default());
    let response = super::response::Head::new(response);
    let write = Box::pin(write::write_grpc_hunks(
        sender,
        write_rx,
        progress.clone(),
        false,
        Some(response.downgrade()),
    ));
    let read = Box::pin(read_response(response, read_tx, multi));
    let stream = GrpcStream::new(
        read_rx,
        tokio_util::sync::PollSender::new(write_tx),
        progress,
    );
    PreparedStream {
        stream,
        read,
        write,
    }
}
async fn read_response(
    resp_future: super::response::Head,
    read_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    multi: bool,
) {
    let resp = match tokio::select! { _ = read_tx.closed() => return, response = futures_util::future::poll_fn(|cx|resp_future.poll(cx)) => response }
    {
        Ok(resp) => resp,
        Err(error) => {
            let _ = read_tx.send(Err(error)).await;
            return;
        }
    };
    let status = resp.status();
    if status != http::StatusCode::OK {
        let _ = read_tx
            .send(Err(io::Error::other(format!("gRPC HTTP status {status}"))))
            .await;
        return;
    }
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !(content_type == "application/grpc"
        || content_type.starts_with("application/grpc+")
        || content_type.starts_with("application/grpc;"))
    {
        let _ = read_tx
            .send(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid gRPC response content type",
            )))
            .await;
        return;
    }
    let header_status = resp.headers().contains_key("grpc-status");
    if header_status {
        if let Err(error) = read::status(resp.headers()) {
            let _ = read_tx.send(Err(error)).await;
            return;
        }
    }
    read_grpc_hunks(resp.into_body(), read_tx, multi, !header_status).await;
}
