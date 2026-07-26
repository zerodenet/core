// Hysteria2 outbound protocol — outbound.rs

use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;

use crate::shared::build_tcp_connect_header;
use crate::udp::{Hysteria2UdpPacket, Hysteria2UdpPacketTarget};
use zero_core::{Error, ProtocolType, Session};
use zero_traits::{AsyncSocket, UdpDatagramFraming};

/// Hysteria2 outbound handler — sends auth and opens streams.
#[derive(Debug, Default, Clone, Copy)]
pub struct Hysteria2Outbound;

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hysteria2OutboundProfile {
    password: String,
    client_fingerprint: Option<String>,
}

#[cfg(feature = "crypto")]
impl Hysteria2OutboundProfile {
    pub fn from_config_parts(password: &str, client_fingerprint: Option<&str>) -> Self {
        Self {
            password: password.to_owned(),
            client_fingerprint: client_fingerprint.map(ToOwned::to_owned),
        }
    }

    pub fn from_config_password(password: &str, client_fingerprint: Option<&str>) -> Self {
        Self::from_config_parts(password, client_fingerprint)
    }

    pub fn client_fingerprint(&self) -> Option<&str> {
        self.client_fingerprint.as_deref()
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    #[cfg(feature = "tokio")]
    pub async fn authenticate_connection<S>(
        &self,
        conn: &quinn::Connection,
        stream: &mut S,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        Hysteria2Outbound
            .authenticate_connection(conn, stream, &self.password)
            .await
    }
}

#[cfg(feature = "crypto")]
pub fn outbound_profile_from_config_password(
    password: &str,
    client_fingerprint: Option<&str>,
) -> Hysteria2OutboundProfile {
    Hysteria2OutboundProfile::from_config_password(password, client_fingerprint)
}

impl Hysteria2Outbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("hysteria2")
    }

    /// Send the authentication frame over a QUIC stream.
    pub async fn send_auth<S: AsyncSocket>(
        &self,
        stream: &mut S,
        hmac: &[u8; 32],
    ) -> Result<(), Error> {
        let frame = crate::shared::build_auth_frame(hmac);
        stream
            .write_all(&frame)
            .await
            .map_err(|_| Error::Io("hysteria2: failed to write auth"))
    }

    /// Compute and send the QUIC-bound authentication frame.
    #[cfg(feature = "crypto")]
    pub async fn authenticate_with_salt<S: AsyncSocket>(
        &self,
        stream: &mut S,
        password: &str,
        salt: &[u8; 32],
    ) -> Result<(), Error> {
        let hmac = crate::shared::sign_hmac(password, salt);
        self.send_auth(stream, &hmac).await?;
        self.read_auth_response(stream).await
    }

    /// Read the authentication response from the server.
    pub async fn read_auth_response<S: AsyncSocket>(&self, stream: &mut S) -> Result<(), Error> {
        let mut buf = [0u8; 64];
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|_| Error::Io("hysteria2: failed to read auth response"))?;
        if n == 0 {
            return Err(Error::Io("hysteria2: EOF reading auth response"));
        }
        crate::shared::parse_auth_response(&buf[..n])
    }

    /// Send a TCP connect request on a new stream.
    pub async fn send_tcp_connect<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<(), Error> {
        let header = build_tcp_connect_header(&session.target, session.port)?;
        stream
            .write_all(&header)
            .await
            .map_err(|_| Error::Io("hysteria2: failed to write connect header"))
    }

    /// Read the TCP connect response.
    pub async fn read_connect_response<S: AsyncSocket>(&self, stream: &mut S) -> Result<(), Error> {
        let mut status = [0u8; 1];
        crate::shared::read_exact(stream, &mut status).await?;
        let message_len = read_varint_from_stream(stream).await?;
        discard_exact(stream, message_len).await?;
        let padding_len = read_varint_from_stream(stream).await?;
        discard_exact(stream, padding_len).await?;
        if status[0] == 0x00 {
            Ok(())
        } else {
            Err(Error::Protocol("hysteria2: connect rejected"))
        }
    }

    #[cfg(all(feature = "tokio", feature = "crypto"))]
    pub async fn authenticate_connection<S>(
        &self,
        conn: &quinn::Connection,
        stream: &mut S,
        password: &str,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let mut salt = [0u8; 32];
        conn.export_keying_material(&mut salt, b"hysteria2 auth", &[])
            .map_err(|_| Error::Io("hysteria2 key export failed"))?;

        self.authenticate_with_salt(stream, password, &salt).await
    }

    pub async fn establish_tcp_connect<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_tcp_connect(stream, session).await?;
        self.read_connect_response(stream).await
    }
}

async fn read_varint_from_stream<S: AsyncSocket>(stream: &mut S) -> Result<usize, Error> {
    let mut first = [0u8; 1];
    crate::shared::read_exact(stream, &mut first).await?;
    let width = 1usize << (first[0] >> 6);
    let mut value = usize::from(first[0] & 0x3f);
    if width > 1 {
        let mut tail = [0u8; 7];
        crate::shared::read_exact(stream, &mut tail[..width - 1]).await?;
        for byte in &tail[..width - 1] {
            value = value
                .checked_shl(8)
                .and_then(|value| value.checked_add(usize::from(*byte)))
                .ok_or(Error::Protocol("hysteria2: QUIC varint overflow"))?;
        }
    }
    Ok(value)
}

async fn discard_exact<S: AsyncSocket>(stream: &mut S, mut remaining: usize) -> Result<(), Error> {
    let mut buffer = [0u8; 256];
    while remaining > 0 {
        let take = remaining.min(buffer.len());
        crate::shared::read_exact(stream, &mut buffer[..take]).await?;
        remaining -= take;
    }
    Ok(())
}

impl<'a> UdpDatagramFraming<Hysteria2UdpPacketTarget<'a>, ()> for Hysteria2Outbound {
    type Error = Error;
    type Decoded = Hysteria2UdpPacket;

    fn encode_udp_datagram(
        &self,
        packet: &Hysteria2UdpPacketTarget<'a>,
    ) -> Result<Vec<u8>, Self::Error> {
        crate::udp::build_udp_datagram(
            packet.session_id,
            packet.packet_id,
            packet.target,
            packet.port,
            packet.payload,
        )
    }

    fn decode_udp_datagram(
        &self,
        _context: &(),
        datagram: &[u8],
    ) -> Result<Self::Decoded, Self::Error> {
        crate::udp::parse_udp_datagram(datagram)
    }
}
