//! Quinn-backed HTTP/3 transport adapter (`h3::quic` traits over `quinn`).

mod stream;

pub use quinn::{self, AcceptBi, AcceptUni, Endpoint, OpenBi, OpenUni, VarInt};
pub use stream::{BidiStream, RecvStream, SendStream};

use bytes::{Buf, Bytes};
use futures_util::{
    ready,
    stream::{unfold, Stream, StreamExt},
};
use h3::{
    error::Code,
    quic::{self, ConnectionErrorIncoming, StreamErrorIncoming},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

type BoxStreamSync<'a, T> = Pin<Box<dyn Stream<Item = T> + Sync + Send + 'a>>;
type IncomingBidiItem =
    Result<(quinn::SendStream, quinn::RecvStream, Bytes), quinn::ConnectionError>;

/// A QUIC connection backed by [`quinn::Connection`] implementing [`h3::quic::Connection`].
pub struct Connection {
    conn: quinn::Connection,
    incoming_bi: BoxStreamSync<'static, IncomingBidiItem>,
    opening_bi: Option<BoxStreamSync<'static, <OpenBi<'static> as Future>::Output>>,
    incoming_uni: BoxStreamSync<'static, <AcceptUni<'static> as Future>::Output>,
    opening_uni: Option<BoxStreamSync<'static, <OpenUni<'static> as Future>::Output>>,
}

impl Connection {
    /// Use a protocol-dispatched incoming stream source. Prefix bytes are replayed
    /// before the remaining receive stream; uni/control streams remain on Quinn.
    pub fn with_incoming_bidi(
        conn: quinn::Connection,
        incoming: impl Stream<Item = IncomingBidiItem> + Send + Sync + 'static,
    ) -> Self {
        let mut connection = Self::new(conn);
        connection.incoming_bi = Box::pin(incoming);
        connection
    }

    /// Create a [`Connection`] from a [`quinn::Connection`].
    pub fn new(conn: quinn::Connection) -> Self {
        Self {
            conn: conn.clone(),
            incoming_bi: Box::pin(unfold(conn.clone(), |conn| async {
                Some((
                    conn.accept_bi()
                        .await
                        .map(|(send, recv)| (send, recv, Bytes::new())),
                    conn,
                ))
            })),
            opening_bi: None,
            incoming_uni: Box::pin(unfold(conn.clone(), |conn| async {
                Some((conn.accept_uni().await, conn))
            })),
            opening_uni: None,
        }
    }
}

impl<B: Buf> quic::Connection<B> for Connection {
    type RecvStream = RecvStream;
    type OpenStreams = OpenStreams;

    fn poll_accept_bidi(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::BidiStream, ConnectionErrorIncoming>> {
        let (send, recv, prefix) = ready!(self.incoming_bi.poll_next_unpin(cx))
            .expect("self.incoming_bi BoxStream never returns None")
            .map_err(convert_connection_error)?;
        Poll::Ready(Ok(Self::BidiStream::new(
            Self::SendStream::new(send),
            Self::RecvStream::with_prefix(recv, prefix),
        )))
    }

    fn poll_accept_recv(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionErrorIncoming>> {
        let recv = ready!(self.incoming_uni.poll_next_unpin(cx))
            .expect("self.incoming_uni BoxStream never returns None")
            .map_err(convert_connection_error)?;
        Poll::Ready(Ok(Self::RecvStream::new(recv)))
    }

    fn opener(&self) -> Self::OpenStreams {
        OpenStreams {
            conn: self.conn.clone(),
            opening_bi: None,
            opening_uni: None,
        }
    }
}

pub(super) fn convert_connection_error(e: quinn::ConnectionError) -> ConnectionErrorIncoming {
    match e {
        quinn::ConnectionError::ApplicationClosed(application_close) => {
            ConnectionErrorIncoming::ApplicationClose {
                error_code: application_close.error_code.into(),
            }
        }
        quinn::ConnectionError::TimedOut => ConnectionErrorIncoming::Timeout,
        error @ quinn::ConnectionError::VersionMismatch
        | error @ quinn::ConnectionError::Reset
        | error @ quinn::ConnectionError::LocallyClosed
        | error @ quinn::ConnectionError::CidsExhausted
        | error @ quinn::ConnectionError::TransportError(_)
        | error @ quinn::ConnectionError::ConnectionClosed(_) => {
            ConnectionErrorIncoming::Undefined(Arc::new(error))
        }
    }
}

impl<B: Buf> quic::OpenStreams<B> for Connection {
    type SendStream = SendStream<B>;
    type BidiStream = BidiStream<B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        let bi = self.opening_bi.get_or_insert_with(|| {
            Box::pin(unfold(self.conn.clone(), |conn| async {
                Some((conn.open_bi().await, conn))
            }))
        });
        let (send, recv) = ready!(bi.poll_next_unpin(cx))
            .expect("BoxStream does not return None")
            .map_err(|e| StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: convert_connection_error(e),
            })?;
        Poll::Ready(Ok(Self::BidiStream::new(
            Self::SendStream::new(send),
            RecvStream::new(recv),
        )))
    }

    fn poll_open_send(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        let uni = self.opening_uni.get_or_insert_with(|| {
            Box::pin(unfold(self.conn.clone(), |conn| async {
                Some((conn.open_uni().await, conn))
            }))
        });
        let send = ready!(uni.poll_next_unpin(cx))
            .expect("BoxStream does not return None")
            .map_err(|e| StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: convert_connection_error(e),
            })?;
        Poll::Ready(Ok(Self::SendStream::new(send)))
    }

    fn close(&mut self, code: Code, reason: &[u8]) {
        self.conn.close(
            VarInt::from_u64(code.value()).expect("error code VarInt"),
            reason,
        );
    }
}

/// Stream opener backed by a [`quinn::Connection`].
pub struct OpenStreams {
    conn: quinn::Connection,
    opening_bi: Option<BoxStreamSync<'static, <OpenBi<'static> as Future>::Output>>,
    opening_uni: Option<BoxStreamSync<'static, <OpenUni<'static> as Future>::Output>>,
}

impl<B: Buf> quic::OpenStreams<B> for OpenStreams {
    type SendStream = SendStream<B>;
    type BidiStream = BidiStream<B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamErrorIncoming>> {
        let bi = self.opening_bi.get_or_insert_with(|| {
            Box::pin(unfold(self.conn.clone(), |conn| async {
                Some((conn.open_bi().await, conn))
            }))
        });
        let (send, recv) = ready!(bi.poll_next_unpin(cx))
            .expect("BoxStream does not return None")
            .map_err(|e| StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: convert_connection_error(e),
            })?;
        Poll::Ready(Ok(Self::BidiStream::new(
            Self::SendStream::new(send),
            RecvStream::new(recv),
        )))
    }

    fn poll_open_send(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamErrorIncoming>> {
        let uni = self.opening_uni.get_or_insert_with(|| {
            Box::pin(unfold(self.conn.clone(), |conn| async {
                Some((conn.open_uni().await, conn))
            }))
        });
        let send = ready!(uni.poll_next_unpin(cx))
            .expect("BoxStream does not return None")
            .map_err(|e| StreamErrorIncoming::ConnectionErrorIncoming {
                connection_error: convert_connection_error(e),
            })?;
        Poll::Ready(Ok(Self::SendStream::new(send)))
    }

    fn close(&mut self, code: Code, reason: &[u8]) {
        self.conn.close(
            VarInt::from_u64(code.value()).expect("error code VarInt"),
            reason,
        );
    }
}

impl Clone for OpenStreams {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            opening_bi: None,
            opening_uni: None,
        }
    }
}
