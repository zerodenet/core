use super::{
    stream::{OwnedRecvStream, OwnedSendStream},
    users::{error, protocol, Permit},
};
use zero_platform_tokio::contract::{ChannelError, Reason, Stage};
/// No raw Connection, Clone or control getter escapes this wrapper. Each
/// actual handle/stream holds its own finite user reservation.
pub struct OwnedConnection {
    pub(super) connection: quinn::Connection,
    pub(super) permit: Permit,
}
impl OwnedConnection {
    pub fn try_clone(&self) -> Result<Self, ChannelError> {
        let permit = self.permit.users().acquire()?;
        Ok(Self {
            connection: self.connection.clone(),
            permit,
        })
    }
    /// Wait only for this endpoint's actual user book, with this connection's stop.
    pub async fn try_clone_wait(&self) -> Result<Self, ChannelError> {
        let permit = tokio::select! { biased;
            source = self.connection.closed() => return Err(protocol(Stage::Prepare, source)),
            permit = self.permit.users().acquire_wait() => permit?,
        };
        Ok(Self {
            connection: self.connection.clone(),
            permit,
        })
    }
    pub fn close(&self, code: quinn::VarInt, reason: &[u8]) {
        self.connection.close(code, reason);
    }
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.connection.remote_address()
    }
    pub fn local_ip(&self) -> Option<std::net::IpAddr> {
        self.connection.local_ip()
    }
    pub async fn closed(&self) -> Result<quinn::ConnectionError, ChannelError> {
        tokio::select! { biased;
            _ = self.permit.users().cancellation().cancelled() => Err(error(Stage::Close, Reason::Cancelled)),
            reason = self.connection.closed() => Ok(reason),
        }
    }
    pub fn negotiate_rate(&self, rate: u64) {
        crate::quic::negotiated::negotiate(&self.connection, rate);
    }
    pub fn export_keying_material(
        &self,
        output: &mut [u8],
        label: &[u8],
        context: &[u8],
    ) -> Result<(), ChannelError> {
        self.connection
            .export_keying_material(output, label, context)
            .map_err(|e| {
                protocol(
                    Stage::Handshake,
                    std::io::Error::other(format!("key export failed: {e:?}")),
                )
            })
    }
    pub fn max_datagram_size(&self) -> Option<usize> {
        self.connection.max_datagram_size()
    }
    pub fn handshake_metadata(&self) -> (Option<String>, Option<String>) {
        crate::quic::handshake_metadata(&self.connection)
    }
    pub fn close_reason(&self) -> Option<quinn::ConnectionError> {
        self.connection.close_reason()
    }
    pub fn send_datagram(&self, bytes: bytes::Bytes) -> Result<(), ChannelError> {
        if self.permit.users().cancellation().is_cancelled() {
            return Err(error(Stage::Send, Reason::Cancelled));
        }
        self.connection
            .send_datagram(bytes)
            .map_err(|source| protocol(Stage::Send, source))
    }
    pub async fn read_datagram(&self) -> Result<bytes::Bytes, ChannelError> {
        tokio::select! { biased;
            _ = self.permit.users().cancellation().cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.read_datagram() => result.map_err(|source| protocol(Stage::Receive, source)),
        }
    }
    pub async fn open_uni(&self) -> Result<OwnedSendStream, ChannelError> {
        let permit = self.permit.users().acquire()?;
        let cancel = permit.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Prepare, Reason::Cancelled)),
            result = self.connection.open_uni() => result.map(|stream| OwnedSendStream::new(stream, permit)).map_err(|source| protocol(Stage::Prepare, source)),
        }
    }
    pub async fn accept_uni(&self) -> Result<OwnedRecvStream, ChannelError> {
        let permit = tokio::select! { biased;
            source = self.connection.closed() => return Err(protocol(Stage::Receive, source)),
            permit = self.permit.users().acquire_wait() => permit?,
        };
        let cancel = permit.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.accept_uni() => result.map(|stream| OwnedRecvStream::new(stream, permit)).map_err(|source| protocol(Stage::Receive, source)),
        }
    }
    pub async fn open_bi(&self) -> Result<(OwnedSendStream, OwnedRecvStream), ChannelError> {
        let (send, recv) = self.permit.users().acquire_pair()?;
        let cancel = send.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Prepare, Reason::Cancelled)),
            result = self.connection.open_bi() => result.map(|(tx, rx)| (OwnedSendStream::new(tx, send), OwnedRecvStream::new(rx, recv))).map_err(|source| protocol(Stage::Prepare, source)),
        }
    }
    pub async fn accept_bi(&self) -> Result<(OwnedSendStream, OwnedRecvStream), ChannelError> {
        let (send, recv) = tokio::select! { biased;
            source = self.connection.closed() => return Err(protocol(Stage::Receive, source)),
            permits = self.permit.users().acquire_pair_wait() => permits?,
        };
        let cancel = send.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.accept_bi() => result.map(|(tx, rx)| (OwnedSendStream::new(tx, send), OwnedRecvStream::new(rx, recv))).map_err(|source| protocol(Stage::Receive, source)),
        }
    }
    /// The classifier's real connection handle and both raw stream views are
    /// admitted together, before accepting a new stream from Quinn.
    pub async fn accept_bi_with_connection(
        &self,
    ) -> Result<(Self, OwnedSendStream, OwnedRecvStream), ChannelError> {
        let (connection, send, recv) = tokio::select! { biased;
            source = self.connection.closed() => return Err(protocol(Stage::Receive, source)),
            permits = self.permit.users().acquire_triplet_wait() => permits?,
        };
        let cancel = connection.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.accept_bi() => result.map(|(tx, rx)| (
                Self { connection: self.connection.clone(), permit: connection },
                OwnedSendStream::new(tx, send), OwnedRecvStream::new(rx, recv)
            )).map_err(|source| protocol(Stage::Receive, source)),
        }
    }
}
