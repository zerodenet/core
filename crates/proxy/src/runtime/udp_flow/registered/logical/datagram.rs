//! One business-flow Datagram session and its retained original controller.
use crate::{
    component::stream_bridge::datagram::WireTraffic, inventory::DatagramAssociationTarget,
    protocol_registry::TcpExecutionServices,
};
use std::{pin::Pin, sync::Arc};
use tokio::sync::Mutex;
use zero_core::{
    Address, Session,
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    ChannelError, CloseReason, Impact, Reason, Stage,
    datagram::{DatagramLimits, DatagramRequest, DatagramRx, DatagramTx, recv, send},
    lifecycle::DynChannelControl,
};
pub(super) struct DatagramConnection {
    tx: Mutex<Pin<Box<dyn DatagramTx>>>,
    rx: Mutex<Pin<Box<dyn DatagramRx>>>,
    control: Mutex<Box<dyn DynChannelControl>>,
    association: AssociationId,
    limits: DatagramLimits,
    wire: Arc<WireTraffic>,
    pub(super) remote: Option<std::net::SocketAddr>,
}
fn legacy(error: ChannelError) -> EngineError {
    std::io::Error::other(error).into()
}
impl DatagramConnection {
    pub(super) async fn open(
        execution: &TcpExecutionServices,
        target: &DatagramAssociationTarget,
        session: &Session,
    ) -> Result<Arc<Self>, EngineError> {
        let slot = target.current_slot(execution)?;
        let reservation = execution.reserve_datagram().map_err(legacy)?;
        let association = reservation.association();
        let opening = reservation
            .begin_open(
                execution,
                vec![slot.role_id()],
                execution
                    .invocation
                    .as_ref()
                    .map(|call| call.cancellation())
                    .unwrap_or_default(),
            )
            .map_err(legacy)?;
        let execution = execution.clone().with_pending_datagram(opening.pending());
        let (tag, server, port) = target.endpoint();
        let address = match zero_core::address::parse_ip_literal(server) {
            Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
            Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
            None => Address::Domain(server.into()),
        };
        let opened = crate::component::stream_bridge::datagram::open_flow(
            &execution,
            &slot,
            tag,
            Destination::new(address, port),
            DatagramRequest {
                association,
                targets: DatagramTargets::Fixed(Destination::new(
                    session.target.clone(),
                    session.port,
                )),
            },
        )
        .await
        .map_err(legacy)?;
        Ok(Self::adopt(opened, opening))
    }
    pub(super) async fn open_relay(
        execution: &TcpExecutionServices,
        prepared: crate::inventory::PreparedDatagramRelay,
        session: &Session,
    ) -> Result<Arc<Self>, EngineError> {
        let reservation = execution.reserve_datagram().map_err(legacy)?;
        let association = reservation.association();
        let mut roles: Vec<_> = prepared
            .prefix
            .iter()
            .map(|node| node.slot.role_id())
            .collect();
        roles.push(prepared.target.current_slot(execution)?.role_id());
        let opening = reservation
            .begin_open(
                execution,
                roles,
                execution
                    .invocation
                    .as_ref()
                    .map(|call| call.cancellation())
                    .unwrap_or_default(),
            )
            .map_err(legacy)?;
        let execution = execution.clone().with_pending_datagram(opening.pending());
        let opened = crate::component::stream_bridge::relay::datagram::open(
            &execution,
            prepared,
            session,
            association,
        )
        .await
        .map_err(legacy)?;
        Ok(Self::adopt(opened, opening))
    }
    fn adopt(
        opened: crate::component::stream_bridge::datagram::Opened,
        reservation: crate::component::datagram_retirement::DatagramOpening,
    ) -> Arc<Self> {
        let association = reservation.association();
        let channel = opened.channel;
        let control = reservation.adopt(channel.control);
        Arc::new(Self {
            tx: Mutex::new(channel.tx),
            rx: Mutex::new(channel.rx),
            control: Mutex::new(control),
            association,
            limits: channel.limits,
            wire: opened.wire,
            remote: opened.remote,
        })
    }
    pub(super) async fn send(
        &self,
        session: &Session,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        let frame = Datagram {
            association: self.association,
            address: Destination::new(session.target.clone(), session.port),
            payload: payload.to_vec().into(),
        };
        let mut tx = self.tx.lock().await;
        let mut control = self.control.lock().await;
        let _ = self.wire.take_flow_sent();
        let result = send(tx.as_mut(), control.as_mut(), &frame, &self.limits).await;
        let encoded = self.wire.take_flow_sent();
        let receipt = result.map_err(|failure| legacy(failure.error))?;
        // Compatibility Flow TX historically includes protocol packet framing.
        // Standard receipts continue to report business payload bytes only.
        Ok(encoded.unwrap_or(receipt.accepted_bytes))
    }
    pub(super) async fn receive(&self) -> Result<super::Packet, EngineError> {
        let frame = recv(self.rx.lock().await.as_mut())
            .await
            .map_err(legacy)?
            .ok_or_else(|| {
                EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Datagram flow reached EOF",
                ))
            })?;
        if frame.association != self.association {
            self.control
                .lock()
                .await
                .cancel(CloseReason::ProtocolFailure);
            return Err(legacy(ChannelError::new(
                Stage::Receive,
                Reason::StalePlan,
                Impact::Session,
            )));
        }
        Ok((
            frame.address.address,
            frame.address.port,
            frame.payload.to_vec(),
        ))
    }
}
impl Drop for DatagramConnection {
    fn drop(&mut self) {
        // This only returns transferred custody. RetiredDatagrams retains the
        // actual original, drives wait_closed, and gates host replacement.
        self.control.get_mut().cancel(CloseReason::Shutdown);
    }
}
