//! Byte view of an existing protocol-owned logical channel. No task or native owner.
use super::*;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;

pub struct ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize>,
    E: FnMut() -> io::Result<()>,
{
    incoming: mpsc::UnboundedReceiver<Vec<u8>>,
    pending: Vec<u8>,
    offset: usize,
    write: W,
    end: Option<E>,
    max_write: NonZeroUsize,
    eof: bool,
}
impl<W, E> ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize>,
    E: FnMut() -> io::Result<()>,
{
    /// Empty messages explicitly end both logical directions; sender closure is upload EOF.
    /// `end` requests logical retirement only; it is never native close proof.
    pub fn new(
        incoming: mpsc::UnboundedReceiver<Vec<u8>>,
        write: W,
        end: E,
        max_write: NonZeroUsize,
        prefix: Vec<u8>,
    ) -> Self {
        Self {
            incoming,
            pending: prefix,
            offset: 0,
            write,
            end: Some(end),
            max_write,
            eof: false,
        }
    }
    fn finish(&mut self) -> io::Result<()> {
        match self.end.take() {
            Some(mut end) => end(),
            None => Ok(()),
        }
    }
}
impl<W, E> Drop for ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize>,
    E: FnMut() -> io::Result<()>,
{
    fn drop(&mut self) {
        let _ = self.finish();
    }
}
impl<W, E> AsyncRead for ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize> + Unpin,
    E: FnMut() -> io::Result<()> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.offset == this.pending.len() && !this.eof {
            match this.incoming.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(bytes)) if !bytes.is_empty() => {
                    this.pending = bytes;
                    this.offset = 0;
                }
                Poll::Ready(Some(_)) => {
                    this.eof = true;
                    this.finish()?;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "logical stream explicitly ended",
                    )));
                }
                Poll::Ready(None) => {
                    this.eof = true;
                }
            }
        }
        let count = output.remaining().min(this.pending.len() - this.offset);
        output.put_slice(&this.pending[this.offset..this.offset + count]);
        this.offset += count;
        Poll::Ready(Ok(()))
    }
}
impl<W, E> AsyncWrite for ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize> + Unpin,
    E: FnMut() -> io::Result<()> + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.end.is_none() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "logical write side ended",
            )));
        }
        let bytes = &bytes[..bytes.len().min(this.max_write.get())];
        Poll::Ready((this.write)(bytes).and_then(|size| {
            if size > bytes.len() {
                Err(io::Error::other("logical writer returned an invalid count"))
            } else {
                Ok(size)
            }
        }))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().finish())
    }
}
impl<W, E> AsyncSocket for ChannelStream<W, E>
where
    W: FnMut(&[u8]) -> io::Result<usize> + Unpin + Send + Sync,
    E: FnMut() -> io::Result<()> + Unpin + Send + Sync,
{
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
