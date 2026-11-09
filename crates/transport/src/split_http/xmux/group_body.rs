//! Actual owned HTTP body: connection/response custody and its precise Box.
use super::*;
use crate::split_http::body::ReceivedBody;
use std::{
    mem::ManuallyDrop,
    task::{Context, Poll},
};
use zero_traits::packet_metadata::{PacketMetadataCredit, PacketMetadataLayout};
struct Body {
    inner: Option<ReceivedBody>,
    connection: Option<connection::Connection>,
    response: Option<group::ResponseUse>,
}
pub(in crate::split_http) struct OwnedGroupBody {
    inner: ManuallyDrop<Box<Body>>,
    memory: Option<PacketMetadataCredit>,
}
impl OwnedGroupBody {
    pub(in crate::split_http) fn layout() -> PacketMetadataLayout {
        PacketMetadataLayout::boxed::<Body>()
    }
    pub(super) fn new(
        body: ReceivedBody,
        connection: connection::Connection,
        response: group::ResponseUse,
        memory: PacketMetadataCredit,
    ) -> Self {
        assert!(memory.layout().bytes() >= Self::layout().bytes());
        Self {
            inner: ManuallyDrop::new(Box::new(Body {
                inner: Some(body),
                connection: Some(connection),
                response: Some(response),
            })),
            memory: Some(memory),
        }
    }
}
impl hyper::body::Body for OwnedGroupBody {
    type Data = bytes::Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<hyper::body::Frame<bytes::Bytes>>>> {
        let result = match self.inner.inner.as_mut() {
            Some(body) => Pin::new(body).poll_frame(cx),
            None => Poll::Ready(None),
        };
        if matches!(result, Poll::Ready(None)) {
            drop(self.inner.inner.take());
            drop(self.inner.connection.take());
            drop(self.inner.response.take());
        }
        result
    }
}
impl Drop for OwnedGroupBody {
    fn drop(&mut self) {
        let memory = self.memory.take();
        // SAFETY: one actual Box, released including its backing before refund.
        let body = unsafe { ManuallyDrop::take(&mut self.inner) };
        drop(body);
        drop(memory);
    }
}
