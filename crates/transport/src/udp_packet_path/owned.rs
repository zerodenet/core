//! Same packet wire over an original socket retained by the caller's owner.
use super::*;
use zero_platform_tokio::controlled_datagram::{
    ControlledDatagramOwner, ControlledDatagramSocket, DatagramReservationId,
};

pub struct OwnedUdpSocketPacketPath {
    socket: ControlledDatagramSocket,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
}
impl OwnedUdpSocketPacketPath {
    /// The actual resource owner must already retain `owner` before this call.
    /// This installs no task, creates no other runtime and grants no pool.
    pub fn establish(
        endpoint: SocketAddr,
        codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
        sockets: &crate::OutboundDatagramSocketFactory,
        owner: &mut ControlledDatagramOwner,
        policy: &zero_traits::DialPolicy,
        expected_generation: u64,
    ) -> Result<Self, RuntimeError> {
        let socket = sockets
            .bind_owned(owner, endpoint, policy, expected_generation)
            .map_err(|source| RuntimeError::Io(std::io::Error::other(source)))?;
        Ok(Self {
            socket,
            endpoint,
            codec,
        })
    }
    pub fn reservation(&self) -> DatagramReservationId {
        self.socket.reservation()
    }
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }
    pub async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), RuntimeError> {
        let packet = self.codec.encode(target, port, payload)?;
        self.socket.send_to_addr(&packet, self.endpoint).await?;
        Ok(())
    }
    pub async fn recv_from(&self, buffer: &mut [u8]) -> Result<usize, RuntimeError> {
        self.recv_from_with_source(buffer)
            .await
            .map(|(size, _, _)| size)
    }
    pub async fn recv_from_with_source(
        &self,
        buffer: &mut [u8],
    ) -> Result<(usize, Address, u16), RuntimeError> {
        let (read, _) = self.socket.recv_from_addr(buffer).await?;
        let (source, port, payload) = self.codec.decode(&buffer[..read]).ok_or_else(|| {
            RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to decode owned UDP packet-path datagram",
            ))
        })?;
        copy_payload(buffer, payload, "owned UDP socket carrier").map(|size| (size, source, port))
    }
}
#[cfg(test)]
mod tests;
