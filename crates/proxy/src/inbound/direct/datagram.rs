//! Opaque datagram forwarding for one peer; authentication remains at the target.
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc, time::Instant};
use zero_core::{
    Address, DatagramUdpResponder, Error, InboundDatagramUdpRelay, InboundUdpDispatch,
    ProtocolType, SessionAuth,
};

pub(super) struct DirectDatagramRelay {
    target: Option<Address>,
    port: u16,
    peer: SocketAddr,
    packets: mpsc::Receiver<Vec<u8>>,
    deadline: Instant,
}

impl DirectDatagramRelay {
    pub(super) fn new(
        target: Option<Address>,
        port: u16,
        peer: SocketAddr,
        packets: mpsc::Receiver<Vec<u8>>,
    ) -> Self {
        Self {
            target,
            port,
            peer,
            packets,
            deadline: Instant::now() + Duration::from_secs(120),
        }
    }
}

impl InboundDatagramUdpRelay<Arc<UdpSocket>> for DirectDatagramRelay {
    type Responder = Self;
    fn into_datagram_udp_parts(self) -> (Self, Option<SessionAuth>) {
        (self, None)
    }
}

#[async_trait::async_trait]
impl DatagramUdpResponder<Arc<UdpSocket>> for DirectDatagramRelay {
    async fn read_inbound_dispatch(
        &mut self,
        _socket: &Arc<UdpSocket>,
    ) -> Result<Option<InboundUdpDispatch>, Error> {
        let Ok(Some(payload)) = tokio::time::timeout_at(self.deadline, self.packets.recv()).await
        else {
            return Ok(None);
        };
        self.deadline = Instant::now() + Duration::from_secs(120);
        Ok(Some(InboundUdpDispatch::new(
            ProtocolType::UNKNOWN,
            match self.target.clone() {
                Some(target) => target,
                None => return Ok(None),
            },
            self.port,
            payload,
            None,
        )))
    }
    async fn write_response_for_session(
        &mut self,
        socket: &Arc<UdpSocket>,
        _session_id: Option<u64>,
        _target: &Address,
        _port: u16,
        payload: &[u8],
    ) -> Result<Option<usize>, Error> {
        self.deadline = Instant::now() + Duration::from_secs(120);
        socket
            .send_to(payload, self.peer)
            .await
            .map(Some)
            .map_err(|_| Error::Io("direct UDP response failed"))
    }
}
