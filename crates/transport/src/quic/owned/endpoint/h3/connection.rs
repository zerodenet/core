use super::*;
use ::h3::{error::Code, quic};
use std::task::ready;

/// H3 borrows one actual sealed Quinn handle. No raw connection is cloned or
/// exposed. Every outgoing/accepted stream still receives its own real leases.
pub struct Connection {
    opener: OpenStreams,
    incoming_bidi: Option<Pending<BidiStream>>,
    incoming_uni: Option<Pending<RecvStream>>,
}
/// Infallible H3 sender clones only borrow the same actual connection handle.
/// The last such borrow releases that handle's original logical reservation.
pub struct OpenStreams {
    opening_bidi: Option<Pending<BidiStream>>,
    opening_uni: Option<Pending<SendStream>>,
    // Pending futures drop before the original connection guard.
    connection: Arc<OwnedConnection>,
}
impl Connection {
    pub fn new(connection: OwnedConnection) -> Self {
        Self {
            opener: OpenStreams {
                opening_bidi: None,
                opening_uni: None,
                connection: Arc::new(connection),
            },
            incoming_bidi: None,
            incoming_uni: None,
        }
    }
}
impl Clone for OpenStreams {
    fn clone(&self) -> Self {
        Self {
            opening_bidi: None,
            opening_uni: None,
            connection: self.connection.clone(),
        }
    }
}
impl quic::Connection<Bytes> for Connection {
    type RecvStream = RecvStream;
    type OpenStreams = OpenStreams;
    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<BidiStream, ConnectionErrorIncoming>> {
        let future = self.incoming_bidi.get_or_insert_with(|| {
            let connection = self.opener.connection.clone();
            Box::pin(async move {
                let (send, recv) = connection.accept_bi().await?;
                Ok(BidiStream::new(send, recv))
            })
        });
        let result = ready!(future.as_mut().poll(cx));
        drop(self.incoming_bidi.take());
        Poll::Ready(result.map_err(connection_error))
    }
    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<RecvStream, ConnectionErrorIncoming>> {
        let future = self.incoming_uni.get_or_insert_with(|| {
            let connection = self.opener.connection.clone();
            Box::pin(async move { connection.accept_uni().await.map(OwnedRecvStream::into_h3) })
        });
        let result = ready!(future.as_mut().poll(cx));
        drop(self.incoming_uni.take());
        Poll::Ready(result.map_err(connection_error))
    }
    fn opener(&self) -> OpenStreams {
        self.opener.clone()
    }
}
impl quic::OpenStreams<Bytes> for OpenStreams {
    type SendStream = SendStream;
    type BidiStream = BidiStream;
    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<BidiStream, StreamErrorIncoming>> {
        let future = self.opening_bidi.get_or_insert_with(|| {
            let connection = self.connection.clone();
            Box::pin(async move {
                let (send, recv) = connection.open_bi().await?;
                Ok(BidiStream::new(send, recv))
            })
        });
        let result = ready!(future.as_mut().poll(cx));
        drop(self.opening_bidi.take());
        Poll::Ready(result.map_err(stream_error))
    }
    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<SendStream, StreamErrorIncoming>> {
        let future = self.opening_uni.get_or_insert_with(|| {
            let connection = self.connection.clone();
            Box::pin(async move { connection.open_uni().await.map(OwnedSendStream::into_h3) })
        });
        let result = ready!(future.as_mut().poll(cx));
        drop(self.opening_uni.take());
        Poll::Ready(result.map_err(stream_error))
    }
    fn close(&mut self, code: Code, reason: &[u8]) {
        self.connection.close(
            quinn::VarInt::from_u64(code.value()).expect("H3 code fits QUIC VarInt"),
            reason,
        );
    }
}
impl quic::OpenStreams<Bytes> for Connection {
    type SendStream = SendStream;
    type BidiStream = BidiStream;
    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<BidiStream, StreamErrorIncoming>> {
        quic::OpenStreams::poll_open_bidi(&mut self.opener, cx)
    }
    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<SendStream, StreamErrorIncoming>> {
        quic::OpenStreams::poll_open_send(&mut self.opener, cx)
    }
    fn close(&mut self, code: Code, reason: &[u8]) {
        quic::OpenStreams::close(&mut self.opener, code, reason)
    }
}
