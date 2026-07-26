// HTTP/2 transport 鈥?h2.rs
//
// Raw DATA frames over HTTP/2 (no gRPC framing).
// Simpler than gRPC transport: bytes flow directly in DATA frames.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Method, Request, Response};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;

use crate::RuntimeError;
use zero_traits::{AsyncSocket, H2TransportProfile};

use zero_platform_tokio::ClientStream;

/// Bidirectional HTTP/2 stream.
pub struct H2Stream {
    read_rx: mpsc::Receiver<Result<Vec<u8>, String>>,
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    read_buffer: Vec<u8>,
    read_offset: usize,
    write_closed: bool,
}

impl H2Stream {
    fn new(
        read_rx: mpsc::Receiver<Result<Vec<u8>, String>>,
        write_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Self {
        Self {
            read_rx,
            write_tx,
            read_buffer: Vec::new(),
            read_offset: 0,
            write_closed: false,
        }
    }
}

// 鈹€鈹€ client (outbound) connect 鈹€鈹€

pub async fn connect_h2<S, TProfile>(
    stream: S,
    h2_config: &TProfile,
    server: &str,
    port: u16,
) -> Result<H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: H2TransportProfile + ?Sized,
{
    let host = h2_config
        .host()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{server}:{port}"));
    let path = if h2_config.path().starts_with('/') {
        h2_config.path().to_owned()
    } else {
        format!("/{}", h2_config.path())
    };

    let request = Request::builder()
        .method(Method::POST)
        .uri(&path)
        .header("host", &host)
        .header("content-type", "application/octet-stream")
        .body(())
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 request build: {e}"))))?;

    connect_h2_request(stream, request).await
}

pub(crate) async fn connect_h2_request<S>(
    stream: S,
    request: Request<()>,
) -> Result<H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut client, conn) = h2::client::handshake(stream)
        .await
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 client handshake: {e}"))))?;

    tokio::spawn(async move {
        if let Err(error) = conn.await {
            tracing::warn!(%error, "h2 client connection error");
        }
    });

    let (response, send_stream) = client
        .send_request(request, false)
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 send request: {e}"))))?;

    Ok(build_h2_client_stream(send_stream, response))
}

// 鈹€鈹€ server (inbound) accept 鈹€鈹€

pub async fn accept_h2<S, TProfile>(
    stream: S,
    h2_config: &TProfile,
) -> Result<H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: H2TransportProfile + ?Sized,
{
    let expected_path = if h2_config.path().starts_with('/') {
        h2_config.path()
    } else {
        "/"
    };
    let mut response = Response::new(());
    response
        .headers_mut()
        .insert("content-type", "application/octet-stream".parse().unwrap());
    accept_h2_with_response(stream, expected_path, response).await
}

pub(crate) async fn accept_h2_with_response<S>(
    stream: S,
    expected_path: &str,
    response: Response<()>,
) -> Result<H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut conn = h2::server::handshake(stream)
        .await
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 server handshake: {e}"))))?;

    let (request, mut respond) = conn
        .accept()
        .await
        .ok_or_else(|| {
            RuntimeError::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "h2 connection closed before request",
            ))
        })?
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 accept: {e}"))))?;

    let got_path = request.uri().path();
    if got_path != expected_path {
        let mut resp = Response::new(());
        *resp.status_mut() = http::StatusCode::NOT_FOUND;
        respond
            .send_response(resp, true)
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 respond: {e}"))))?;
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("h2 path mismatch: expected {expected_path}, got {got_path}"),
        )));
    }

    let send_stream = respond
        .send_response(response, false)
        .map_err(|e| RuntimeError::Io(io::Error::other(format!("h2 respond: {e}"))))?;

    let recv_stream = request.into_body();
    tokio::spawn(async move {
        while let Some(result) = conn.accept().await {
            match result {
                Ok((_, mut respond)) => {
                    let mut response = Response::new(());
                    *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                    let _ = respond.send_response(response, true);
                }
                Err(error) => {
                    tracing::debug!(%error, "h2 server connection failed");
                    return;
                }
            }
        }
    });

    build_h2_stream(send_stream, recv_stream)
}

// 鈹€鈹€ common H2 stream builder 鈹€鈹€

fn build_h2_stream(
    send_stream: h2::SendStream<Bytes>,
    recv_stream: h2::RecvStream,
) -> Result<H2Stream, RuntimeError> {
    let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (read_tx, read_rx) = mpsc::channel::<Result<Vec<u8>, String>>(32);

    spawn_h2_write_relay(send_stream, write_rx);
    spawn_h2_read_relay(recv_stream, read_tx);

    Ok(H2Stream::new(read_rx, write_tx))
}

fn build_h2_client_stream(
    send_stream: h2::SendStream<Bytes>,
    response: h2::client::ResponseFuture,
) -> H2Stream {
    let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (read_tx, read_rx) = mpsc::channel::<Result<Vec<u8>, String>>(32);

    spawn_h2_write_relay(send_stream, write_rx);
    tokio::spawn(async move {
        let response = match response.await {
            Ok(response) => response,
            Err(error) => {
                let _ = read_tx
                    .send(Err(format!("h2 response failed: {error}")))
                    .await;
                return;
            }
        };
        let status = response.status();
        if !status.is_success() {
            let _ = read_tx
                .send(Err(format!(
                    "h2 server returned non-success status {status}"
                )))
                .await;
            return;
        }
        read_h2_data(response.into_body(), read_tx).await;
    });

    H2Stream::new(read_rx, write_tx)
}

fn spawn_h2_write_relay(
    mut send_stream: h2::SendStream<Bytes>,
    mut write_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    tokio::spawn(async move {
        while let Some(data) = write_rx.recv().await {
            if data.is_empty() {
                let _ = send_stream.send_data(Bytes::new(), true);
                return;
            }
            if send_stream.send_data(Bytes::from(data), false).is_err() {
                return;
            }
        }
        let _ = send_stream.send_data(Bytes::new(), true);
    });
}

fn spawn_h2_read_relay(
    recv_stream: h2::RecvStream,
    read_tx: mpsc::Sender<Result<Vec<u8>, String>>,
) {
    tokio::spawn(read_h2_data(recv_stream, read_tx));
}

async fn read_h2_data(
    mut recv_stream: h2::RecvStream,
    read_tx: mpsc::Sender<Result<Vec<u8>, String>>,
) {
    loop {
        match recv_stream.data().await {
            Some(Ok(data)) => {
                let _ = recv_stream.flow_control().release_capacity(data.len());
                if read_tx.send(Ok(data.to_vec())).await.is_err() {
                    return;
                }
            }
            Some(Err(error)) => {
                let _ = read_tx
                    .send(Err(format!("h2 response body failed: {error}")))
                    .await;
                return;
            }
            None => return,
        }
    }
}

// 鈹€鈹€ AsyncRead / AsyncWrite / AsyncSocket / ClientStream 鈹€鈹€

impl AsyncRead for H2Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.read_offset < self.read_buffer.len() {
            let available = self.read_buffer.len() - self.read_offset;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&self.read_buffer[self.read_offset..self.read_offset + to_copy]);
            self.read_offset += to_copy;
            if self.read_offset >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match self.read_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                let to_copy = data.len().min(buf.remaining());
                buf.put_slice(&data[..to_copy]);
                if to_copy < data.len() {
                    self.read_buffer = data;
                    self.read_offset = to_copy;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionAborted, error)))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for H2Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "h2 write side closed",
            )));
        }
        match self.write_tx.send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "h2 write side closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if !self.write_closed {
            self.write_closed = true;
            let _ = self.write_tx.send(Vec::new());
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncSocket for H2Stream {
    type Error = io::Error;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl ClientStream for H2Stream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "H2Stream does not expose local_addr",
        ))
    }
}
