//! One compatibility borrow for the existing protocol-owned wire choreography.
//! The original pinned stream is returned afterwards, without a pipe or task.
use std::{io, sync::Mutex};
use zero_platform_tokio::contract::stream::BoxStream;
use zero_traits::AsyncSocket;

pub(super) struct Socket {
    io: Mutex<BoxStream>,
    source: Option<io::Error>,
}
impl Socket {
    pub(super) fn new(io: BoxStream) -> Self {
        Self {
            io: Mutex::new(io),
            source: None,
        }
    }
    pub(super) fn take_source(&mut self) -> Option<io::Error> {
        self.source.take()
    }
    pub(super) fn into_inner(self) -> BoxStream {
        self.io.into_inner().expect("no shared lock access")
    }
    fn stream(&mut self) -> &mut BoxStream {
        self.io.get_mut().expect("exclusive protocol access")
    }
    fn capture<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        result.map_err(|source| {
            let kind = source.kind();
            self.source = Some(source);
            io::Error::new(kind, "HTTP CONNECT carrier I/O failed")
        })
    }
}
impl AsyncSocket for Socket {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let result = tokio::io::AsyncReadExt::read(self.stream(), bytes).await;
        self.capture(result)
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let result = tokio::io::AsyncWriteExt::write_all(self.stream(), bytes).await;
        self.capture(result)
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        let result = tokio::io::AsyncWriteExt::shutdown(self.stream()).await;
        self.capture(result)
    }
}
