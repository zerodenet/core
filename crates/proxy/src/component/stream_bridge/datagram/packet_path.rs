//! Existing packet-path consumers select the actual standard Datagram role.
//! This stores only a weak generation reference, never a protocol plan or pool.
use crate::{
    component::{
        instance::{invalid, ComponentSlot},
        stream_bridge::network::NodeOpener,
    },
    inventory::StreamRoleRef,
    protocol_registry::PacketPathExecutionServices,
    runtime::{
        udp_dispatch::packet_path_operation::{
            PacketPathCarrierFuture, PreparedUdpPacketPathOperation,
        },
        udp_flow::packet_path::{PacketPathCarrier, PacketPathCarrierDescriptor},
    },
};
use std::{pin::Pin, sync::Arc};
use zero_core::{
    component::{AssociationId, NetworkGeneration},
    request::{DatagramTargets, Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramLimits, DatagramRequest, DatagramRx, DatagramTx},
    lifecycle::DynChannelControl,
    network::{UpstreamBinding, UpstreamTransport},
    CancellationToken, ChannelError, CloseReason, Reason,
};
pub(crate) struct StandardPacketPath {
    selected: StreamRoleRef,
    descriptor: PacketPathCarrierDescriptor,
}
impl StandardPacketPath {
    pub(crate) fn prepare(
        slot: &Arc<ComponentSlot>,
        tag: &str,
        server: &str,
        port: u16,
    ) -> Arc<dyn PreparedUdpPacketPathOperation> {
        Arc::new(Self {
            selected: StreamRoleRef::new(slot),
            descriptor: PacketPathCarrierDescriptor {
                tag: Some(tag.into()),
                server: server.into(),
                port,
                cache_key: format!("standard-datagram:{:?}", slot.role_id()),
            },
        })
    }
}
fn failure(error: ChannelError) -> EngineError {
    EngineError::Io(std::io::Error::other(error))
}
impl PreparedUdpPacketPathOperation for StandardPacketPath {
    fn retained_admission(&self) -> Result<(), EngineError> {
        self.selected.slot().map(|_| ()).map_err(failure)
    }
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        Some(self.descriptor.clone())
    }
    fn build_carrier<'a>(
        &'a self,
        services: PacketPathExecutionServices,
    ) -> PacketPathCarrierFuture<'a> {
        Box::pin(async move {
            let execution = services.tcp();
            let slot = self.selected.slot().map_err(failure)?;
            let tag = self.descriptor.tag.as_deref().unwrap();
            let current = execution
                .components
                .as_ref()
                .map_err(|source| EngineError::Io(std::io::Error::other(source.clone())))?
                .configured(tag)
                .ok_or_else(|| {
                    failure(invalid(
                        Reason::Revoked,
                        "standard packet-path owner retired",
                    ))
                })?;
            if !self.selected.matches(&current) {
                return Err(failure(invalid(
                    Reason::StalePlan,
                    "standard packet-path generation changed",
                )));
            }
            let end = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let role = slot.datagram(end).await.map_err(failure)?;
            let endpoint = Destination::new(
                zero_core::Address::Domain(self.descriptor.server.clone()),
                self.descriptor.port,
            );
            let root = NodeOpener::new(execution.upstream_observed(tag), endpoint);
            let reservation = execution.reserve_datagram().map_err(failure)?;
            let association = reservation.association();
            let stop = CancellationToken::new();
            let mut session = role
                .open(
                    DatagramRequest {
                        association,
                        targets: DatagramTargets::Multiple,
                    },
                    OpenContext {
                        identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                            origin: ServiceOrigin::Runtime,
                            purpose: ServicePurpose::BackgroundControl,
                        })),
                        deadline: Some(end),
                        cancel: stop.clone(),
                        upstream: UpstreamTransport::new(
                            UpstreamBinding {
                                prefix: vec![],
                                dial: slot.dial.clone(),
                                network_generation: NetworkGeneration(
                                    execution.upstream().network_generation(),
                                ),
                            },
                            Arc::new(root),
                        ),
                        observer: None,
                    },
                )
                .await
                .map_err(failure)?;
            session.control = reservation.adopt(session.control);
            Ok(Arc::new(Path::new(session, association, stop)) as Arc<dyn PacketPathCarrier>)
        })
    }
}
struct Path {
    tx: tokio::sync::Mutex<Pin<Box<dyn DatagramTx>>>,
    rx: tokio::sync::Mutex<Pin<Box<dyn DatagramRx>>>,
    control: tokio::sync::Mutex<Box<dyn DynChannelControl>>,
    limits: DatagramLimits,
    association: AssociationId,
    stop: CancellationToken,
}
impl Path {
    fn new(
        session: BoxDatagramSession,
        association: AssociationId,
        stop: CancellationToken,
    ) -> Self {
        Self {
            tx: tokio::sync::Mutex::new(session.tx),
            rx: tokio::sync::Mutex::new(session.rx),
            control: tokio::sync::Mutex::new(session.control),
            limits: session.limits,
            association,
            stop,
        }
    }
}
#[async_trait::async_trait]
impl PacketPathCarrier for Path {
    async fn send_to(
        &self,
        target: &zero_core::Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let frame = zero_core::packet::Datagram {
            association: self.association,
            address: Destination::new(target.clone(), port),
            payload: payload.to_vec().into(),
        };
        let mut tx = self.tx.lock().await;
        let mut control = self.control.lock().await;
        zero_platform_tokio::contract::datagram::send(
            tx.as_mut(),
            control.as_mut(),
            &frame,
            &self.limits,
        )
        .await
        .map_err(|source| EngineError::Io(std::io::Error::other(source)))?;
        Ok(())
    }
    async fn recv_from(&self, bytes: &mut [u8]) -> Result<usize, EngineError> {
        let mut rx = self.rx.lock().await;
        let frame = zero_platform_tokio::contract::datagram::recv(rx.as_mut())
            .await
            .map_err(failure)?
            .ok_or_else(|| {
                failure(invalid(
                    Reason::Revoked,
                    "standard packet-path receiver closed",
                ))
            })?;
        if frame.payload.len() > bytes.len() {
            return Err(failure(invalid(
                Reason::Capacity,
                "standard packet-path receive buffer too small",
            )));
        }
        bytes[..frame.payload.len()].copy_from_slice(&frame.payload);
        Ok(frame.payload.len())
    }
}
impl Drop for Path {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Ok(control) = self.control.try_lock() {
            control.cancel(CloseReason::Shutdown);
        }
    }
}
