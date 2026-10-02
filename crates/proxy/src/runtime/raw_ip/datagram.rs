//! One-shot datagram exchange through a protocol-neutral shared raw-IP device.

use std::{io, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use zero_stack::{
    client_udp::ClientUdpEvent,
    packet::{Endpoint, IcmpErrorKind},
};

use super::{RawIpDevicePool, RawIpOutboundPlan};
use crate::runtime::packet_route::PreparedDatagramExchangeOperation;

pub(crate) struct RawIpDatagramOperation {
    pub(crate) tag: String,
    pub(crate) identity: [u8; 32],
    pub(crate) plan: Arc<dyn RawIpOutboundPlan>,
    pub(crate) pool: Arc<RawIpDevicePool>,
}

#[async_trait]
impl PreparedDatagramExchangeOperation for RawIpDatagramOperation {
    async fn exchange(
        &self,
        endpoint: SocketAddr,
        payload: Vec<u8>,
        egress_generation: u64,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<Vec<u8>> {
        let peer = self
            .plan
            .peer_for_target(endpoint.ip())
            .map_err(io::Error::other)?;
        let cell = self
            .pool
            .cell_for(&self.tag, peer.peer_index, self.identity, egress_generation)
            .map_err(io::Error::other)?;
        let device = cell
            .get()
            .ok_or_else(|| io::Error::other("raw-IP peer device not initialized"))?;
        if !self.pool.is_current(
            &self.tag,
            peer.peer_index,
            self.identity,
            egress_generation,
            device,
        ) {
            return Err(io::Error::other("raw-IP peer device changed"));
        }
        let mut socket = device
            .bind_udp_observed(peer.local_ip, observer)
            .map_err(|error| io::Error::other(format!("raw-IP UDP stack: {error:?}")))?;
        let target = Endpoint {
            ip: endpoint.ip(),
            port: endpoint.port(),
        };
        socket
            .send_to(&payload, target)
            .await
            .map_err(|error| io::Error::other(format!("raw-IP UDP stack: {error:?}")))?;
        loop {
            match socket.recv_event().await {
                Some(ClientUdpEvent::Datagram(response)) if response.source == target => {
                    return Ok(response.payload);
                }
                Some(ClientUdpEvent::Datagram(_)) => continue,
                Some(ClientUdpEvent::IcmpError(error)) => match error.kind {
                    IcmpErrorKind::PacketTooBig { .. } => continue,
                    kind => {
                        return Err(io::Error::other(format!("raw-IP UDP ICMP error: {kind:?}")))
                    }
                },
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "raw-IP UDP socket closed",
                    ))
                }
            }
        }
    }
}
