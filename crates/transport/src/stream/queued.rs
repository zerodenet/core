//! Byte view of one existing bounded protocol FIFO. No worker or native owner.
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};

type Reservation<T> =
    Pin<Box<dyn Future<Output = Result<mpsc::OwnedPermit<T>, mpsc::error::SendError<()>>> + Send>>;

/// The protocol owns serialization and its byte-credit reservation. DATA is
/// accepted only when both that credit and the original FIFO frame permit exist.
/// A cancelled Pending poll never stores or sends its caller's payload.
pub struct QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>>,
    Abandon: FnMut(),
{
    incoming: mpsc::UnboundedReceiver<Vec<u8>>,
    pending: Vec<u8>,
    offset: usize,
    eof: bool,
    aborted: bool,
    sender: mpsc::Sender<T>,
    reserving: Option<Reservation<T>>,
    permit: Option<mpsc::OwnedPermit<T>>,
    encode: Encode,
    abandon: Option<Abandon>,
    maximum: NonZeroUsize,
}
impl<T: Send + 'static, Encode, Abandon> QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>>,
    Abandon: FnMut(),
{
    pub fn new(
        incoming: mpsc::UnboundedReceiver<Vec<u8>>,
        sender: mpsc::Sender<T>,
        encode: Encode,
        abandon: Abandon,
        maximum: NonZeroUsize,
        prefix: Vec<u8>,
    ) -> Self {
        Self {
            incoming,
            pending: prefix,
            offset: 0,
            eof: false,
            aborted: false,
            sender,
            reserving: None,
            permit: None,
            encode,
            abandon: Some(abandon),
            maximum,
        }
    }
    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
        bytes: Option<&[u8]>,
    ) -> Poll<io::Result<usize>> {
        if self.permit.is_none() {
            let reserve = self
                .reserving
                .get_or_insert_with(|| Box::pin(self.sender.clone().reserve_owned()));
            match reserve.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.reserving = None;
                    self.permit = Some(result.map_err(io::Error::other)?);
                }
            }
        }
        let bytes = bytes.map(|bytes| &bytes[..bytes.len().min(self.maximum.get())]);
        match (self.encode)(cx, bytes) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                // No frame was accepted. Return the same actual reservation.
                drop(self.permit.take());
                Poll::Ready(Err(error))
            }
            Poll::Ready(Ok(frame)) => {
                self.permit
                    .take()
                    .expect("reserved original FIFO")
                    .send(frame);
                if bytes.is_none() {
                    self.abandon = None;
                }
                Poll::Ready(Ok(bytes.map_or(0, <[u8]>::len)))
            }
        }
    }
}
impl<T, Encode, Abandon> Drop for QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>>,
    Abandon: FnMut(),
{
    fn drop(&mut self) {
        // Release a cancelled write's own reservation before best-effort abort.
        drop(self.permit.take());
        drop(self.reserving.take());
        if let Some(mut abandon) = self.abandon.take() {
            abandon();
        }
    }
}
impl<T, Encode, Abandon> Unpin for QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>> + Unpin,
    Abandon: FnMut() + Unpin,
{
}
impl<T, Encode, Abandon> AsyncRead for QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>> + Unpin,
    Abandon: FnMut() + Unpin,
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
                    this.aborted = true;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "logical stream explicitly ended",
                    )));
                }
                // Sender close is upload EOF, independent from downlink END.
                Poll::Ready(None) => this.eof = true,
            }
        }
        let count = output.remaining().min(this.pending.len() - this.offset);
        output.put_slice(&this.pending[this.offset..this.offset + count]);
        this.offset += count;
        Poll::Ready(Ok(()))
    }
}
impl<T: Send + 'static, Encode, Abandon> AsyncWrite for QueuedChannelStream<T, Encode, Abandon>
where
    Encode: FnMut(&mut Context<'_>, Option<&[u8]>) -> Poll<io::Result<T>> + Unpin,
    Abandon: FnMut() + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this.abandon.is_none() || this.aborted {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        this.poll_frame(cx, Some(bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // This is the original logical FIFO handoff, never native flush proof.
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.abandon.is_none() {
            return Poll::Ready(Ok(()));
        }
        this.poll_frame(cx, None).map(|result| result.map(|_| ()))
    }
}

#[cfg(test)]
#[path = "queued/tests.rs"]
mod tests;
