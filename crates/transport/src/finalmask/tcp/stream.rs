use super::*;
use crate::stream::progress::Progress;
use bytes::Bytes;
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};
use zero_platform_tokio::ClientStream;
pub(super) enum Transform {
    Fragment(Fragment),
    Sudoku(super::super::sudoku::Profile, bool),
}
pub(super) struct Stream {
    input: mpsc::Receiver<io::Result<Bytes>>,
    pending: Bytes,
    eof: bool,
    output: tokio_util::sync::PollSender<Bytes>,
    progress: Arc<Progress>,
    driver: tokio::task::AbortHandle,
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
}
impl Stream {
    pub(super) fn new(raw: TcpRelayStream, transform: Transform) -> Self {
        let local = raw.local_addr().ok();
        let peer = raw.peer_addr().ok();
        let (input_tx, input) = mpsc::channel(16);
        let (output_tx, output) = mpsc::channel::<Bytes>(16);
        let progress = Arc::new(Progress::default());
        let state = progress.clone();
        let task = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(raw);
            let (mut decoder, mut encoder) = match transform {
                Transform::Fragment(config) => (Decoder::Copy, Encoder::Fragment(config, 0)),
                Transform::Sudoku(profile, true) => (
                    Decoder::Pure(profile.decoder()),
                    Encoder::Packed(profile.packed_encoder()),
                ),
                Transform::Sudoku(profile, false) => (
                    Decoder::Packed(profile.packed_decoder()),
                    Encoder::Pure(profile.encoder()),
                ),
            };
            let read = async {
                let mut buffer = vec![0; 32768];
                loop {
                    let n = reader.read(&mut buffer).await?;
                    if n == 0 {
                        let _ = input_tx.send(Ok(Bytes::new())).await;
                        return Ok::<_, io::Error>(());
                    }
                    let decoded = decoder.decode(&buffer[..n])?;
                    if !decoded.is_empty() && input_tx.send(Ok(Bytes::from(decoded))).await.is_err()
                    {
                        return Ok(());
                    }
                }
            };
            let write = async {
                let mut output = output;
                while let Some(bytes) = output.recv().await {
                    encoder.write(&mut writer, &bytes).await?;
                    writer.flush().await?;
                    state.commit(bytes.len());
                }
                writer.shutdown().await?;
                state.finish();
                Ok::<_, io::Error>(())
            };
            if let Err(error) = tokio::try_join!(read, write) {
                state.fail(&error);
                let _ = input_tx.send(Err(error)).await;
            }
        });
        Self {
            input,
            pending: Bytes::new(),
            eof: false,
            output: tokio_util::sync::PollSender::new(output_tx),
            progress,
            driver: task.abort_handle(),
            local,
            peer,
        }
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        self.driver.abort();
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.pending.is_empty() && !self.eof {
            match std::task::ready!(self.input.poll_recv(cx)) {
                Some(Ok(bytes)) if !bytes.is_empty() => self.pending = bytes,
                Some(Err(error)) => return Poll::Ready(Err(error)),
                _ => self.eof = true,
            }
        }
        let length = self.pending.len().min(buf.remaining());
        buf.put_slice(&self.pending.split_to(length));
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.progress.check()?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        std::task::ready!(self.output.poll_reserve(cx))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        let n = buf.len().min(65536);
        self.output
            .send_item(Bytes::copy_from_slice(&buf[..n]))
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        self.progress.accept(n);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.progress.poll(cx, false)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.output.close();
        self.progress.poll(cx, true)
    }
}
impl zero_traits::AsyncSocket for Stream {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl ClientStream for Stream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local
            .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer
            .ok_or_else(|| io::Error::from(io::ErrorKind::Unsupported))
    }
}
enum Decoder {
    Copy,
    Pure(super::super::sudoku::Decoder),
    Packed(super::super::sudoku::PackedDecoder),
}
impl Decoder {
    fn decode(&mut self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        match self {
            Self::Copy => Ok(bytes.to_vec()),
            Self::Pure(decoder) => decoder.decode(bytes),
            Self::Packed(decoder) => Ok(decoder.decode(bytes)),
        }
    }
}
enum Encoder {
    Fragment(Fragment, u64),
    Pure(super::super::sudoku::Encoder),
    Packed(super::super::sudoku::PackedEncoder),
}
impl Encoder {
    async fn write<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        bytes: &[u8],
    ) -> io::Result<()> {
        match self {
            Self::Fragment(config, count) => {
                *count = count.saturating_add(1);
                fragment::write(writer, config, *count, bytes).await
            }
            Self::Pure(encoder) => writer.write_all(&encoder.encode(bytes)).await,
            Self::Packed(encoder) => writer.write_all(&encoder.encode(bytes)).await,
        }
    }
}
