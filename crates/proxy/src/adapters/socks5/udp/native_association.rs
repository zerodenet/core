//! The legacy shared-association holder now invokes the actual Datagram role.
mod control;
use crate::{
    protocol_registry::UdpRuntimeServices,
    runtime::udp_flow::registered::{UpstreamAssociationCloseReason, UpstreamAssociationTransport},
};
use ::socks5::transport::Socks5ManagedUdpAssociationTarget;
use control::ObservedControl;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};
use tokio::sync::Mutex;
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address, Session,
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    datagram::{recv, send, DatagramLimits, DatagramRequest, DatagramRx, DatagramTx},
    lifecycle::{erase_control, DynChannelControl},
    ChannelError, CloseReason, Impact, Reason, Stage,
};
pub(super) struct NativeAssociation {
    tx: Mutex<Pin<Box<dyn DatagramTx>>>,
    rx: Mutex<Pin<Box<dyn DatagramRx>>>,
    control: Mutex<Box<dyn DynChannelControl>>,
    limits: DatagramLimits,
    association: AssociationId,
    reason: Arc<AtomicU8>,
    wire: Arc<crate::component::stream_bridge::datagram::WireTraffic>,
}
fn legacy(error: ChannelError) -> EngineError {
    EngineError::Io(std::io::Error::other(error))
}
fn address(server: &str) -> Address {
    match zero_core::address::parse_ip_literal(server) {
        Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
        None => Address::Domain(server.to_owned()),
    }
}
#[async_trait::async_trait]
impl UpstreamAssociationTransport<Socks5ManagedUdpAssociationTarget> for NativeAssociation {
    async fn establish(
        services: UdpRuntimeServices,
        target: Socks5ManagedUdpAssociationTarget,
        session: &Session,
        inbound_tag: &str,
    ) -> Result<Self, EngineError> {
        let execution = services.datagram_execution(inbound_tag, session)?;
        let (tag, server, port) = target.log_parts();
        let input = execution
            .config()
            .outbounds
            .iter()
            .find(|input| input.tag == tag)
            .ok_or_else(|| {
                legacy(ChannelError::new(
                    Stage::Prepare,
                    Reason::StalePlan,
                    Impact::Session,
                ))
            })?;
        if input.protocol.endpoint() != Some((server, port)) {
            return Err(legacy(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Session,
            )));
        }
        if let Some((reason, endpoint)) =
            zero_engine::EndpointAdmission::from_config(execution.config()).outbound_denial(tag)
        {
            return Err(EngineError::InvalidPlan {
                message: format!("{reason}: {endpoint}"),
            });
        }
        let plan = execution
            .components
            .as_ref()
            .map_err(|error| legacy(crate::component::instance::retained(error.clone())))?;
        let slot = plan.configured(tag).ok_or_else(|| {
            legacy(ChannelError::new(
                Stage::Prepare,
                Reason::Unsupported,
                Impact::Session,
            ))
        })?;
        // Reserve before any role admission can open its native TCP carrier.
        let reservation = execution.retired_datagrams().reserve().map_err(legacy)?;
        let association = reservation.association();
        let opened = crate::component::stream_bridge::datagram::open(
            &execution,
            &slot,
            tag,
            Destination::new(address(server), port),
            DatagramRequest {
                association,
                targets: DatagramTargets::Multiple,
            },
        )
        .await
        .map_err(legacy)?;
        let channel = opened.channel;
        let reason = Arc::new(AtomicU8::new(0));
        let actual = erase_control(ObservedControl::new(
            channel.control,
            services.network(),
            reason.clone(),
        ));
        let control = reservation.adopt(actual);
        Ok(Self {
            tx: Mutex::new(channel.tx),
            rx: Mutex::new(channel.rx),
            control: Mutex::new(control),
            limits: channel.limits,
            association,
            reason,
            wire: opened.wire,
        })
    }
    async fn send_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        let frame = Datagram {
            address: Destination::new(target.clone(), port),
            association: self.association,
            payload: payload.to_vec().into(),
        };
        let mut tx = self.tx.lock().await;
        let mut control = self.control.lock().await;
        self.wire.take_sent();
        let result = send(tx.as_mut(), control.as_mut(), &frame, &self.limits).await;
        let actual_wire = self.wire.take_sent();
        result.map_err(|failure| legacy(failure.error))?;
        if actual_wire == 0 || actual_wire < payload.len() {
            let mut error = ChannelError::new(Stage::Send, Reason::Internal, Impact::Session);
            error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
            control.cancel(CloseReason::DeliveryUnknown);
            return Err(legacy(error));
        }
        // The standard receipt remains business payload bytes. This temporary
        // legacy caller historically meters actual SOCKS5 UDP wire TX; consume
        // the native observer once rather than reconstructing a protocol header.
        Ok(actual_wire)
    }
    async fn recv_response_parts(
        &self,
        buf: &mut [u8],
    ) -> Result<(Address, u16, Vec<u8>), EngineError> {
        let mut rx = self.rx.lock().await;
        let frame = recv(rx.as_mut()).await.map_err(legacy)?.ok_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "SOCKS5 Datagram session reached actual EOF",
            ))
        })?;
        if frame.association != self.association {
            return Err(legacy(ChannelError::new(
                Stage::Receive,
                Reason::StalePlan,
                Impact::Session,
            )));
        }
        if frame.payload.len() > buf.len() {
            return Err(legacy(ChannelError::new(
                Stage::Receive,
                Reason::Capacity,
                Impact::Datagram,
            )));
        }
        Ok((
            frame.address.address,
            frame.address.port,
            frame.payload.to_vec(),
        ))
    }
    fn close(mut self, reason: UpstreamAssociationCloseReason) {
        let (value, close) = match reason {
            UpstreamAssociationCloseReason::Closed => (1, CloseReason::Completed),
            UpstreamAssociationCloseReason::IdleTimeout => (2, CloseReason::Completed),
            UpstreamAssociationCloseReason::Dropped => (3, CloseReason::ProtocolFailure),
        };
        let _ = self
            .reason
            .compare_exchange(0, value, Ordering::AcqRel, Ordering::Acquire);
        self.control.get_mut().cancel(close);
        // Drop only the reference lease. The actual controller and its future
        // remain in the application owner until an observed complete report.
    }
}
impl Drop for NativeAssociation {
    fn drop(&mut self) {
        let _ = self
            .reason
            .compare_exchange(0, 3, Ordering::AcqRel, Ordering::Acquire);
        self.control.get_mut().cancel(CloseReason::Shutdown);
    }
}
