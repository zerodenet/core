//! Sealed H3 adapter over the original owned Quinn connection and stream leases.
//! Existing H3/HY2 pools and native mask/hop paths are not activated by this API.
use super::{OwnedConnection, OwnedRecvStream, OwnedSendStream};
use ::h3::quic::{ConnectionErrorIncoming, StreamErrorIncoming};
use bytes::Bytes;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_platform_tokio::contract::ChannelError;
mod bidi;
mod connection;
mod outcome;
mod prepared;
pub use prepared::{PendingHttp3, RequestSender};
mod streams;
pub use bidi::BidiStream;
pub use connection::{Connection, OpenStreams};
pub use outcome::{Http3DriverExit, Http3DriverFailure};
pub use streams::{RecvStream, SendStream};

type Pending<T> = Pin<Box<dyn Future<Output = Result<T, ChannelError>> + Send + Sync>>;
fn connection_error(error: ChannelError) -> ConnectionErrorIncoming {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(quinn::ConnectionError::ApplicationClosed(close)) = current.downcast_ref() {
            return ConnectionErrorIncoming::ApplicationClose {
                error_code: close.error_code.into_inner(),
            };
        }
        if matches!(
            current.downcast_ref::<quinn::ConnectionError>(),
            Some(quinn::ConnectionError::TimedOut)
        ) {
            return ConnectionErrorIncoming::Timeout;
        }
        source = current.source();
    }
    // The complete typed original ChannelError remains in H3's actual error
    // payload, rather than a replacement display string or invented IO error.
    ConnectionErrorIncoming::Undefined(Arc::new(error))
}
fn stream_error(error: ChannelError) -> StreamErrorIncoming {
    StreamErrorIncoming::ConnectionErrorIncoming {
        connection_error: connection_error(error),
    }
}
