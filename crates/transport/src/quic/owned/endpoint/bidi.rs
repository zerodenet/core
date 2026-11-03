//! A sealed bidirectional stream retains the actual Connection user as well as
//! both actual stream users. No raw Quinn handle escapes type erasure.
use super::{OwnedConnection, OwnedRecvStream, OwnedSendStream};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_platform_tokio::contract::ChannelError;

pub struct OwnedBiStream {
    receive: OwnedRecvStream,
    send: OwnedSendStream,
    // Drop stream originals and their leases before the connection reference.
    connection: OwnedConnection,
}
impl OwnedConnection {
    pub async fn accept_bi_stream(self) -> Result<OwnedBiStream, ChannelError> {
        let (send, receive) = self.accept_bi().await?;
        Ok(OwnedBiStream {
            receive,
            send,
            connection: self,
        })
    }
    pub async fn into_bi_stream(self) -> Result<OwnedBiStream, ChannelError> {
        let (send, receive) = self.open_bi().await?;
        Ok(OwnedBiStream {
            receive,
            send,
            connection: self,
        })
    }
}
impl OwnedBiStream {
    pub fn from_streams(
        connection: OwnedConnection,
        send: OwnedSendStream,
        receive: OwnedRecvStream,
    ) -> Self {
        Self {
            connection,
            send,
            receive,
        }
    }
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.connection.remote_address()
    }
}
impl AsyncRead for OwnedBiStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().receive).poll_read(cx, bytes)
    }
}
impl AsyncWrite for OwnedBiStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().send).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().send).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().send).poll_shutdown(cx)
    }
}

impl zero_traits::AsyncSocket for OwnedBiStream {
    type Error = io::Error;
    async fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, b).await
    }
    async fn write_all(&mut self, b: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, b).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl zero_platform_tokio::ClientStream for OwnedBiStream {
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "local endpoint comes from the original source facts",
        ))
    }
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        Ok(self.remote_address())
    }
}
