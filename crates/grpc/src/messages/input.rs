//! Read the original gRPC length prefix before its existing decoder allocates.
use super::Record;
use hyper::body::{Body, Bytes, Frame, Incoming, SizeHint};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
pub(crate) struct Input {
    inner: Incoming,
    original: Arc<Record>,
    header: [u8; 5],
    used: usize,
    remaining: usize,
}
impl Input {
    pub(crate) fn new(inner: Incoming, original: Arc<Record>) -> Result<Self, tonic::Status> {
        original.start_input()?;
        Ok(Self {
            inner,
            original,
            header: [0; 5],
            used: 0,
            remaining: 0,
        })
    }
    fn observe(&mut self, mut bytes: &[u8]) -> Result<(), tonic::Status> {
        while !bytes.is_empty() {
            if self.remaining != 0 {
                let count = bytes.len().min(self.remaining);
                self.remaining -= count;
                bytes = &bytes[count..];
                continue;
            }
            let count = bytes.len().min(5 - self.used);
            self.header[self.used..self.used + count].copy_from_slice(&bytes[..count]);
            self.used += count;
            bytes = &bytes[count..];
            if self.used == 5 {
                // Compression/invalid flag status remains the existing
                // decoder's responsibility. This observer only accounts for
                // the original length prefix, without changing the wire.
                let count = u32::from_be_bytes(self.header[1..].try_into().unwrap()) as usize;
                self.original.input_message(count)?;
                self.remaining = count;
                self.used = 0;
            }
        }
        Ok(())
    }
}
impl Body for Input {
    type Data = Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        match std::task::ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(bytes) = frame.data_ref() {
                    if let Err(error) = this.observe(bytes) {
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(error)) => Poll::Ready(Some(Err(tonic::Status::from_error(Box::new(error))))),
            None => Poll::Ready(None),
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
