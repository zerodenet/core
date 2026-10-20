use super::super::users::Permit;
use super::*;
use ::h3::quic::{self, StreamId, WriteBuf};
use bytes::Buf;
use tokio_util::sync::WaitForCancellationFutureOwned;

/// The original adapter (including its pending raw read-chunk future) is
/// destroyed before returning its actual receive-direction logical capacity.
pub struct RecvStream {
    stream: h3_quinn::RecvStream,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    _permit: Permit,
}
/// The original raw send stream and buffered frame are destroyed before its
/// actual send-direction lease is released. FIN alone never refunds capacity.
pub struct SendStream {
    stream: h3_quinn::SendStream<Bytes>,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    _permit: Permit,
}
impl RecvStream {
    pub(in crate::quic::owned::endpoint) fn new(
        stream: quinn::RecvStream,
        cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
        permit: Permit,
    ) -> Self {
        Self {
            stream: h3_quinn::RecvStream::new(stream),
            cancelled,
            _permit: permit,
        }
    }
}
impl SendStream {
    pub(in crate::quic::owned::endpoint) fn new(
        stream: quinn::SendStream,
        cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
        permit: Permit,
    ) -> Self {
        Self {
            stream: h3_quinn::SendStream::new(stream),
            cancelled,
            _permit: permit,
        }
    }
}
fn cancelled(
    future: &mut Pin<Box<WaitForCancellationFutureOwned>>,
    cx: &mut Context<'_>,
) -> Result<(), StreamErrorIncoming> {
    if future.as_mut().poll(cx).is_ready() {
        Err(StreamErrorIncoming::Unknown(Box::new(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "original owned H3 stream cancelled",
        ))))
    } else {
        Ok(())
    }
}
impl quic::RecvStream for RecvStream {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Bytes>, StreamErrorIncoming>> {
        cancelled(&mut self.cancelled, cx)?;
        quic::RecvStream::poll_data(&mut self.stream, cx)
    }
    fn stop_sending(&mut self, code: u64) {
        quic::RecvStream::stop_sending(&mut self.stream, code);
    }
    fn recv_id(&self) -> StreamId {
        quic::RecvStream::recv_id(&self.stream)
    }
}
impl quic::SendStream<Bytes> for SendStream {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        cancelled(&mut self.cancelled, cx)?;
        quic::SendStream::poll_ready(&mut self.stream, cx)
    }
    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        if self._permit.users().cancellation().is_cancelled() {
            return Err(StreamErrorIncoming::Unknown(Box::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "original owned H3 stream cancelled",
            ))));
        }
        quic::SendStream::send_data(&mut self.stream, data)
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        cancelled(&mut self.cancelled, cx)?;
        quic::SendStream::poll_finish(&mut self.stream, cx)
    }
    fn reset(&mut self, code: u64) {
        quic::SendStream::reset(&mut self.stream, code);
    }
    fn send_id(&self) -> StreamId {
        quic::SendStream::send_id(&self.stream)
    }
}
impl quic::SendStreamUnframed<Bytes> for SendStream {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        data: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        cancelled(&mut self.cancelled, cx)?;
        quic::SendStreamUnframed::poll_send(&mut self.stream, cx, data)
    }
}
