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
        let permit = self.permit.users().acquire()?;
        let cancel = permit.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.accept_uni() => result.map(|stream| OwnedRecvStream::new(stream, permit)).map_err(|source| protocol(Stage::Receive, source)),
        }
    }
    pub async fn open_bi(&self) -> Result<(OwnedSendStream, OwnedRecvStream), ChannelError> {
        let send = self.permit.users().acquire()?;
        let recv = self.permit.users().acquire()?;
        let cancel = send.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Prepare, Reason::Cancelled)),
            result = self.connection.open_bi() => result.map(|(tx, rx)| (OwnedSendStream::new(tx, send), OwnedRecvStream::new(rx, recv))).map_err(|source| protocol(Stage::Prepare, source)),
        }
    }
    pub async fn accept_bi(&self) -> Result<(OwnedSendStream, OwnedRecvStream), ChannelError> {
        let send = self.permit.users().acquire()?;
        let recv = self.permit.users().acquire()?;
        let cancel = send.users().cancellation().clone();
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
            result = self.connection.accept_bi() => result.map(|(tx, rx)| (OwnedSendStream::new(tx, send), OwnedRecvStream::new(rx, recv))).map_err(|source| protocol(Stage::Receive, source)),
        }
    }
}
