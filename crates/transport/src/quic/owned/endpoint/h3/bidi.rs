use super::*;
use ::h3::quic::{self, StreamId, WriteBuf};
use bytes::Buf;
pub struct BidiStream {
    send: SendStream,
    recv: RecvStream,
}
impl BidiStream {
    pub(super) fn new(send: OwnedSendStream, recv: OwnedRecvStream) -> Self {
        Self {
            send: send.into_h3(),
            recv: recv.into_h3(),
        }
    }
}
impl quic::BidiStream<Bytes> for BidiStream {
    type SendStream = SendStream;
    type RecvStream = RecvStream;
    fn split(self) -> (SendStream, RecvStream) {
        (self.send, self.recv)
    }
}
impl quic::RecvStream for BidiStream {
    type Buf = Bytes;
    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Bytes>, StreamErrorIncoming>> {
        quic::RecvStream::poll_data(&mut self.recv, cx)
    }
    fn stop_sending(&mut self, code: u64) {
        quic::RecvStream::stop_sending(&mut self.recv, code);
    }
    fn recv_id(&self) -> StreamId {
        quic::RecvStream::recv_id(&self.recv)
    }
}
impl quic::SendStream<Bytes> for BidiStream {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        quic::SendStream::poll_ready(&mut self.send, cx)
    }
    fn send_data<T: Into<WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), StreamErrorIncoming> {
        quic::SendStream::send_data(&mut self.send, data)
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamErrorIncoming>> {
        quic::SendStream::poll_finish(&mut self.send, cx)
    }
    fn reset(&mut self, code: u64) {
        quic::SendStream::reset(&mut self.send, code);
    }
    fn send_id(&self) -> StreamId {
        quic::SendStream::send_id(&self.send)
    }
}
impl quic::SendStreamUnframed<Bytes> for BidiStream {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        data: &mut D,
    ) -> Poll<Result<usize, StreamErrorIncoming>> {
        quic::SendStreamUnframed::poll_send(&mut self.send, cx, data)
    }
}
