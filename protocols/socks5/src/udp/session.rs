use zero_core::{Address, Error, InboundUdpAssociationResponse};
use zero_traits::{DatagramSocket, IpAddress, SocketAddress};

use super::association::Socks5UdpRelayError;
use super::dispatch::Socks5InboundUdpSession;
use super::packet::Socks5InboundUdpDispatchAction;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Socks5InboundUdpResponder {
    session: Socks5InboundUdpSession,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Socks5InboundUdpAssociationSession {
    relay_session: Socks5InboundUdpRelaySession,
    responder: Socks5InboundUdpResponder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Socks5InboundUdpRelayPacketAction<'a> {
    ClientPacket {
        payload: &'a [u8],
    },
    PeerResponse {
        sender: SocketAddress,
        payload: &'a [u8],
    },
    UnexpectedSender {
        sender: SocketAddress,
    },
}

impl crate::inbound::Socks5UdpAssociateRequest {
    pub fn client_endpoint_hint(&self) -> Option<SocketAddress> {
        if self.client_port == 0 {
            return None;
        }

        match &self.client_hint {
            Address::Ipv4(ip) if *ip != [0, 0, 0, 0] => Some(SocketAddress {
                ip: IpAddress::V4(*ip),
                port: self.client_port,
            }),
            Address::Ipv6(ip) if *ip != [0; 16] => Some(SocketAddress {
                ip: IpAddress::V6(*ip),
                port: self.client_port,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Socks5InboundUdpRelaySession {
    client: Option<SocketAddress>,
}

impl Socks5InboundUdpRelaySession {
    fn new() -> Self {
        Self::default()
    }

    fn from_associate_request(request: &crate::inbound::Socks5UdpAssociateRequest) -> Self {
        Self {
            client: request.client_endpoint_hint(),
        }
    }

    fn client(&self) -> Option<SocketAddress> {
        self.client
    }

    fn classify_packet<'a>(
        &mut self,
        sender: SocketAddress,
        payload: &'a [u8],
    ) -> Socks5InboundUdpRelayPacketAction<'a> {
        if self.client.is_none() {
            self.client = Some(sender);
        }

        match self.client {
            Some(client) if client == sender => {
                Socks5InboundUdpRelayPacketAction::ClientPacket { payload }
            }
            Some(_client) => Socks5InboundUdpRelayPacketAction::PeerResponse { sender, payload },
            None => Socks5InboundUdpRelayPacketAction::UnexpectedSender { sender },
        }
    }
}

impl Socks5InboundUdpResponder {
    fn new() -> Self {
        Self {
            session: Socks5InboundUdpSession::new(),
        }
    }

    async fn send_client_response_for_target<S>(
        &self,
        socket: &S,
        client: SocketAddress,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        self.session
            .send_client_response_for_target(
                socket,
                client,
                upstream_address,
                upstream_port,
                payload,
            )
            .await
    }
}

impl Socks5InboundUdpAssociationSession {
    pub fn new() -> Self {
        Self {
            relay_session: Socks5InboundUdpRelaySession::new(),
            responder: Socks5InboundUdpResponder::new(),
        }
    }

    pub fn from_associate_request(request: crate::inbound::Socks5UdpAssociateRequest) -> Self {
        Self {
            relay_session: Socks5InboundUdpRelaySession::from_associate_request(&request),
            responder: Socks5InboundUdpResponder::new(),
        }
    }

    /// Decode once into the standard inbound packet facts. No routing, DNS I/O,
    /// accounting or socket ownership belongs to this protocol step.
    pub fn decode_datagram_event(
        &mut self,
        sender: SocketAddress,
        packet: &[u8],
    ) -> Result<Option<zero_platform_tokio::contract::inbound::InboundDatagramEvent>, Error> {
        use zero_platform_tokio::contract::inbound::InboundDatagramEvent as Event;
        match self.classify_relay_packet(sender, packet) {
            Socks5InboundUdpRelayPacketAction::ClientPacket { payload } => {
                let action = self
                    .responder
                    .session
                    .decode_request(payload)?
                    .into_dispatch_action();
                Ok(Some(match action {
                    Socks5InboundUdpDispatchAction::LocalDns { domain } => {
                        Event::Resolve { domain }
                    }
                    Socks5InboundUdpDispatchAction::Dispatch(view) => Event::Packet {
                        protocol_bytes: view.protocol_overhead_bytes(),
                        dispatch: view.into_inbound_dispatch(),
                        sender: zero_platform_tokio::socket_address_to_socket_addr(sender),
                    },
                }))
            }
            Socks5InboundUdpRelayPacketAction::PeerResponse { sender, payload } => {
                Ok(Some(Event::PeerResponse {
                    sender: zero_platform_tokio::socket_address_to_socket_addr(sender),
                    payload: payload.to_vec(),
                }))
            }
            Socks5InboundUdpRelayPacketAction::UnexpectedSender { .. } => Ok(None),
        }
    }

    pub fn classify_relay_packet<'a>(
        &mut self,
        sender: SocketAddress,
        payload: &'a [u8],
    ) -> Socks5InboundUdpRelayPacketAction<'a> {
        self.relay_session.classify_packet(sender, payload)
    }

    fn client(&self) -> Option<SocketAddress> {
        self.relay_session.client()
    }

    pub fn dispatch_relay_packet_with<FClient, FPeer, FUnexpected>(
        &mut self,
        sender: SocketAddress,
        packet: &[u8],
        on_client_packet: FClient,
        on_peer_response: FPeer,
        on_unexpected_sender: FUnexpected,
    ) where
        FClient: FnOnce(&[u8]),
        FPeer: FnOnce(SocketAddress, &[u8]),
        FUnexpected: FnOnce(SocketAddress),
    {
        match self.classify_relay_packet(sender, packet) {
            Socks5InboundUdpRelayPacketAction::ClientPacket { payload } => {
                on_client_packet(payload);
            }
            Socks5InboundUdpRelayPacketAction::PeerResponse { sender, payload } => {
                on_peer_response(sender, payload);
            }
            Socks5InboundUdpRelayPacketAction::UnexpectedSender { sender } => {
                on_unexpected_sender(sender);
            }
        }
    }

    async fn send_client_response_for_target<S>(
        &self,
        socket: &S,
        client: SocketAddress,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        self.responder
            .send_client_response_for_target(
                socket,
                client,
                upstream_address,
                upstream_port,
                payload,
            )
            .await
    }

    pub async fn send_current_client_response_for_target<S>(
        &self,
        socket: &S,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        let Some(client) = self.client() else {
            return Ok(0);
        };
        self.send_client_response_for_target(
            socket,
            client,
            upstream_address,
            upstream_port,
            payload,
        )
        .await
    }

    pub async fn send_current_client_peer_response_parts<S>(
        &self,
        socket: &S,
        sender: SocketAddress,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        let Some(client) = self.client() else {
            return Ok(0);
        };
        let (target, port) = socket_address_response_target(sender);
        self.send_client_response_for_target(socket, client, &target, port, payload)
            .await
    }
}

impl Socks5InboundUdpAssociationSession {
    pub fn build_response_for_target(
        &self,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<Option<InboundUdpAssociationResponse>, Error> {
        let Some(client) = self.client() else {
            return Ok(None);
        };
        let packet = self.responder.session.encode_response_to_client(
            upstream_address,
            upstream_port,
            payload,
        )?;
        Ok(Some(InboundUdpAssociationResponse::new(client, packet)))
    }

    pub fn build_peer_response(
        &self,
        sender: SocketAddress,
        payload: &[u8],
    ) -> Result<Option<InboundUdpAssociationResponse>, Error> {
        let (target, port) = socket_address_response_target(sender);
        self.build_response_for_target(&target, port, payload)
    }
}

impl crate::inbound::Socks5Inbound {
    pub fn accept_udp_association(
        &self,
        request: crate::inbound::Socks5UdpAssociateRequest,
    ) -> Socks5InboundUdpAssociationSession {
        Socks5InboundUdpAssociationSession::from_associate_request(request)
    }
}

impl Socks5InboundUdpSession {
    async fn send_response_to_client<S>(
        &self,
        socket: &S,
        client: IpAddress,
        client_port: u16,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        let packet = self
            .encode_response_to_client(upstream_address, upstream_port, payload)
            .map_err(Socks5UdpRelayError::Protocol)?;
        socket
            .send_to(&packet, client, client_port)
            .await
            .map_err(Socks5UdpRelayError::Socket)?;
        Ok(packet.len())
    }

    async fn send_client_response_for_target<S>(
        &self,
        socket: &S,
        client: SocketAddress,
        upstream_address: &Address,
        upstream_port: u16,
        payload: &[u8],
    ) -> Result<usize, Socks5UdpRelayError<S::Error>>
    where
        S: DatagramSocket,
    {
        self.send_response_to_client(
            socket,
            client.ip,
            client.port,
            upstream_address,
            upstream_port,
            payload,
        )
        .await
    }
}

fn socket_address_response_target(sender: SocketAddress) -> (Address, u16) {
    let target = match sender.ip {
        IpAddress::V4(ip) => Address::Ipv4(ip),
        IpAddress::V6(ip) => Address::Ipv6(ip),
    };
    (target, sender.port)
}
