use std::io;
use std::net::SocketAddr;

use crate::RuntimeError;

use super::{QuicInbound, QuicStream};

impl QuicInbound {
    pub fn local_addr(&self) -> Result<SocketAddr, RuntimeError> {
        self.endpoint.local_addr().map_err(RuntimeError::Io)
    }

    /// Wait for the endpoint to yield a new connection attempt.
    ///
    /// This boundary intentionally does not await the client handshake. A
    /// failed handshake belongs to one remote connection and must not be
    /// confused with termination of the listening endpoint.
    pub async fn accept_incoming(&self) -> Option<quinn::Incoming> {
        self.endpoint.accept().await
    }

    pub async fn establish_incoming(
        incoming: quinn::Incoming,
    ) -> Result<quinn::Connection, RuntimeError> {
        incoming
            .await
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic connection: {e}"))))
    }

    pub async fn establish_incoming_stream(
        incoming: quinn::Incoming,
    ) -> Result<QuicStream, RuntimeError> {
        let connection = Self::establish_incoming(incoming).await?;
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|e| RuntimeError::Io(io::Error::other(format!("quic accept stream: {e}"))))?;
        Ok(QuicStream::new(send, recv))
    }

    pub async fn accept(&self) -> Result<QuicStream, RuntimeError> {
        let incoming = self.accept_incoming().await.ok_or_else(endpoint_closed)?;
        Self::establish_incoming_stream(incoming).await
    }

    /// Accept a raw QUIC connection for callers that need multi-stream support
    /// and key export.
    pub async fn accept_connection(&self) -> Result<quinn::Connection, RuntimeError> {
        let incoming = self.accept_incoming().await.ok_or_else(endpoint_closed)?;
        Self::establish_incoming(incoming).await
    }
}

fn endpoint_closed() -> RuntimeError {
    RuntimeError::Io(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "quic endpoint closed",
    ))
}
