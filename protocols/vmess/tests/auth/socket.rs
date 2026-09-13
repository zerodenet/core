use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_core::{Address, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;
pub(super) struct Socket {
    input: io::Cursor<Vec<u8>>,
    pub(super) output: Arc<Mutex<Vec<u8>>>,
}
impl Socket {
    pub(super) fn new(data: Vec<u8>) -> Self {
        Self {
            input: io::Cursor::new(data),
            output: Default::default(),
        }
    }
}
impl AsyncSocket for Socket {
    type Error = io::Error;
    async fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        io::Read::read(&mut self.input, b)
    }
    async fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
        self.output.lock().unwrap().extend_from_slice(b);
        Ok(())
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut bytes = vec![0; b.remaining()];
        let n = io::Read::read(&mut self.input, &mut bytes)?;
        b.put_slice(&bytes[..n]);
        Poll::Ready(Ok(()))
    }
}
impl AsyncWrite for Socket {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, b: &[u8]) -> Poll<io::Result<usize>> {
        self.output.lock().unwrap().extend_from_slice(b);
        Poll::Ready(Ok(b.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
pub(super) fn target() -> Session {
    Session::new(
        0,
        Address::Domain("example.com".into()),
        443,
        Network::Tcp,
        ProtocolType::new("audit"),
    )
}
