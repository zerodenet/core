use super::*;
#[cfg(feature = "crypto")]
pub struct ShadowsocksInboundUdpRelay {
    pub(super) responder: ShadowsocksInboundUdpResponder,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpRelay {
    pub fn from_profile(profile: crate::inbound::ShadowsocksInboundProfile) -> Self {
        Self {
            responder: ShadowsocksInboundUdpResponder::from_profile(profile),
        }
    }

    fn into_parts(self) -> ShadowsocksInboundUdpResponder {
        self.responder
    }
}

#[cfg(feature = "crypto")]
impl InboundDatagramUdpRelay<std::sync::Arc<tokio::net::UdpSocket>> for ShadowsocksInboundUdpRelay {
    type Responder = ShadowsocksInboundUdpResponder;

    fn into_datagram_udp_parts(self) -> (Self::Responder, Option<SessionAuth>) {
        (self.into_parts(), None)
    }
}

/// Decoded Shadowsocks inbound UDP request.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksInboundUdpPacket {
    pub(super) target: Address,
    pub(super) port: u16,
    pub(super) payload: Vec<u8>,
    /// Flow isolation id for SIP022/2022 UDP sessions. `None` for legacy AEAD.
    pub(super) client_session_id: Option<u64>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpPacket {
    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn client_session_id(&self) -> Option<u64> {
        self.client_session_id
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        (self.target, self.port, self.payload, self.client_session_id)
    }

    pub fn into_dispatch_parts(self) -> ShadowsocksInboundUdpDispatchParts {
        let (target, port, payload, client_session_id) = self.into_parts();
        ShadowsocksInboundUdpDispatchParts {
            target,
            port,
            payload,
            client_session_id,
        }
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksInboundUdpDispatchParts {
    pub(super) target: Address,
    pub(super) port: u16,
    pub(super) payload: Vec<u8>,
    pub(super) client_session_id: Option<u64>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpDispatchParts {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("shadowsocks")
    }

    pub fn pipe_parts(&self) -> (&Address, u16, &[u8], Option<u64>) {
        (
            &self.target,
            self.port,
            &self.payload,
            self.client_session_id,
        )
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>, Option<u64>) {
        (self.target, self.port, self.payload, self.client_session_id)
    }

    pub fn into_inbound_dispatch(self) -> InboundUdpDispatch {
        InboundUdpDispatch::new(
            ProtocolType::new("shadowsocks"),
            self.target,
            self.port,
            self.payload,
            self.client_session_id,
        )
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksInboundUdpResponse {
    pub(super) datagram: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpResponse {
    pub fn datagram(&self) -> &[u8] {
        &self.datagram
    }

    pub fn into_datagram(self) -> Vec<u8> {
        self.datagram
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksInboundUdpResponseTarget {
    pub(super) client_session_id: Option<u64>,
    pub(super) target: Address,
    pub(super) port: u16,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpResponseTarget {
    pub fn new(client_session_id: Option<u64>, target: Address, port: u16) -> Self {
        Self {
            client_session_id,
            target,
            port,
        }
    }

    pub fn from_parts(client_session_id: Option<u64>, target: &Address, port: u16) -> Self {
        Self::new(client_session_id, target.clone(), port)
    }
}

#[cfg(feature = "crypto")]
pub struct ShadowsocksInboundUdpClientResponse<'a> {
    pub(super) target: &'a Address,
    pub(super) port: u16,
    pub(super) payload: &'a [u8],
}

#[cfg(feature = "crypto")]
impl<'a> ShadowsocksInboundUdpClientResponse<'a> {
    pub fn new(target: &'a Address, port: u16, payload: &'a [u8]) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    pub(super) fn target(&self) -> &'a Address {
        self.target
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

#[cfg(feature = "crypto")]
pub struct ShadowsocksInboundUdpSession {
    pub(super) codec: ShadowsocksInboundUdpCodec,
    pub(super) bindings: state::Bindings,
}

#[cfg(feature = "crypto")]
pub struct ShadowsocksInboundUdpResponder {
    pub(super) mode: ShadowsocksInboundUdpResponderMode,
    pub(super) pending_client: Option<std::net::SocketAddr>,
    pub(super) pending_wire_session: Option<u64>,
    pub(super) associations: association::Associations,
    pub(super) read_buf: Vec<u8>,
    pub(super) maintenance_at: tokio::time::Instant,
}

#[cfg(feature = "crypto")]
pub(super) enum ShadowsocksInboundUdpResponderMode {
    Single(ShadowsocksInboundUdpSession),
    Profile {
        profile: crate::inbound::ShadowsocksInboundProfile,
        sessions: std::collections::HashMap<String, ShadowsocksInboundUdpSession>,
        proxy_users: std::collections::HashMap<u64, String>,
        current_user: Option<String>,
        current_auth: Option<SessionAuth>,
    },
}

#[cfg(feature = "crypto")]
#[derive(Debug)]
pub struct ShadowsocksInboundUdpResponseDatagram {
    pub(super) datagram: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksInboundUdpResponseDatagram {
    pub fn len(&self) -> usize {
        self.datagram.len()
    }

    pub fn is_empty(&self) -> bool {
        self.datagram.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.datagram
    }

    pub fn into_datagram(self) -> Vec<u8> {
        self.datagram
    }
}
