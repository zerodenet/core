use super::{client_config, AlpnClass};
use crate::reality::target::Profile;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::{ClientStream, TcpRelayStream};
use zero_transport::handshake_target::Connector;

const POST_HANDSHAKE_WINDOW: Duration = Duration::from_secs(5);
const MAX_POST_HANDSHAKE_BYTES: usize = 128 * 1024;
const MAX_POST_HANDSHAKE_RECORDS: usize = 64;

pub(super) async fn connect_target(
    profile: &Profile,
    connector: &Connector,
) -> io::Result<TcpRelayStream> {
    let mut target = connector.connect(profile.endpoint.clone()).await?;
    let prefix = zero_transport::proxy_protocol::encode(
        profile.proxy_protocol,
        target.local_addr().ok(),
        target.peer_addr().ok(),
    )?;
    target.write_all(&prefix).await?;
    Ok(target)
}

pub(super) async fn observe_post_handshake(
    profile: &Profile,
    connector: &Connector,
    server_name: &str,
    alpn: AlpnClass,
) -> io::Result<Vec<usize>> {
    let raw = Arc::new(Mutex::new(Capture::default()));
    let target = RecordingIo {
        inner: connect_target(profile, connector).await?,
        reads: raw.clone(),
    };
    let mut stream =
        ztls::stream::Tls13Stream::connect_async(target, client_config(server_name, alpn)).await?;
    let mut buffer = [0; 1024];
    let _ = tokio::time::timeout(POST_HANDSHAKE_WINDOW, async {
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    let raw = raw.lock().unwrap_or_else(|error| error.into_inner());
    if raw.overflowed || !raw.active {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "target post-handshake capture exceeded its bound",
        ));
    }
    post_handshake_lengths(&raw.bytes)
}

pub(super) fn post_handshake_lengths(raw: &[u8]) -> io::Result<Vec<usize>> {
    let mut offset = 0;
    let mut lengths = Vec::new();
    while raw.len() >= offset + 5 {
        let header = &raw[offset..offset + 5];
        if header[..3] != [23, 3, 3] {
            break;
        }
        let length = 5 + u16::from_be_bytes([header[3], header[4]]) as usize;
        if !(22..=16645).contains(&length) || raw.len() < offset + length {
            break;
        }
        if lengths.len() >= MAX_POST_HANDSHAKE_RECORDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "too many target post-handshake records",
            ));
        }
        lengths.push(length);
        offset += length;
    }
    Ok(lengths)
}

#[derive(Default)]
pub(super) struct Capture {
    pub(super) bytes: Vec<u8>,
    pub(super) active: bool,
    overflowed: bool,
}

pub(super) struct RecordingIo<S> {
    pub(super) inner: S,
    pub(super) reads: Arc<Mutex<Capture>>,
}

impl<S: AsyncRead + Unpin> AsyncRead for RecordingIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let previous = buffer.filled().len();
        match Pin::new(&mut self.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let bytes = &buffer.filled()[previous..];
                let mut capture = self.reads.lock().unwrap_or_else(|error| error.into_inner());
                if capture.active && !bytes.is_empty() {
                    if capture.bytes.len().saturating_add(bytes.len()) > MAX_POST_HANDSHAKE_BYTES {
                        capture.overflowed = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "target post-handshake capture is too large",
                        )));
                    }
                    capture.bytes.extend_from_slice(bytes);
                }
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RecordingIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if super::ccs::starts_client_finished_flight(buffer) {
            let mut capture = self.reads.lock().unwrap_or_else(|error| error.into_inner());
            if !capture.active {
                capture.bytes.clear();
                capture.active = true;
            }
        }
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}
