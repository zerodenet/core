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
    component::PacketDriverDatagramHandoff,
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramLimits, DatagramRequest, DatagramRx, DatagramTx},
    network::{UpstreamBinding, UpstreamTransport},
    CancellationToken, ChannelError, CloseReason, Reason,
};
mod business;
mod path;
pub(crate) mod relay;
use path::Path;
pub(crate) struct StandardPacketPath {
    selected: StreamRoleRef,
    descriptor: PacketPathCarrierDescriptor,
    associated: bool,
}
impl StandardPacketPath {
    /// Existing claimed operation supplies the exact old capability/grammar;
    /// the actual installed role supplies its own generation and IO owner.
    pub(crate) async fn build_installed(
        operation: &dyn PreparedUdpPacketPathOperation,
        services: &PacketPathExecutionServices,
    ) -> Result<Option<Arc<dyn PacketPathCarrier>>, EngineError> {
        if services.has_packet_driver_caller() || !services.has_business_invocation() {
            return Ok(None);
        }
        let Some(descriptor) = operation.carrier_descriptor() else {
            return Ok(None);
        };
        let Some(tag) = descriptor.tag.as_deref() else {
            return Ok(None);
        };
        let execution = services.tcp();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| failure(crate::component::instance::retained(source.clone())))?;
        let slot = plan
            .configured(tag)
            .ok_or_else(|| failure(invalid(Reason::Revoked, "packet carrier owner retired")))?;
        if !slot
            .recipe
            .spec()
            .roles
            .contains(&zero_platform_tokio::contract::component::RoleKind::Datagram)
        {
            // An old legal transport still without an owning Factory remains
            // an explicit compatibility caller. This is not a library grant.
            return Ok(None);
        }
        let selected = Self {
            selected: StreamRoleRef::new(&slot),
            descriptor,
            associated: operation.supports_associated_relay_carrier(),
        };
        selected.build_carrier(services.clone()).await.map(Some)
    }
    pub(crate) fn prepare(
        slot: &Arc<ComponentSlot>,
        tag: &str,
        server: &str,
        port: u16,
    ) -> Arc<dyn PreparedUdpPacketPathOperation> {
        Arc::new(Self {
            selected: StreamRoleRef::new(slot),
            associated: false,
            descriptor: PacketPathCarrierDescriptor {
                tag: Some(tag.into()),
                server: server.into(),
                port,
                cache_key: format!("standard-datagram:{:?}", slot.role_id()),
            },
        })
    }
    pub(crate) fn prepare_projection(
        slot: &Arc<ComponentSlot>,
        projection: &crate::protocol_registry::ColdPacketPathProjection,
    ) -> Arc<dyn PreparedUdpPacketPathOperation> {
        Arc::new(Self {
            selected: StreamRoleRef::new(slot),
            descriptor: projection.descriptor.clone(),
            associated: projection.associated,
        })
    }
}
fn failure(error: ChannelError) -> EngineError {
    EngineError::Io(std::io::Error::other(error))
}
impl PreparedUdpPacketPathOperation for StandardPacketPath {
    fn supports_associated_relay_carrier(&self) -> bool {
        self.associated
    }
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
            let binding = UpstreamBinding {
                prefix: vec![],
                dial: slot.dial.clone(),
                network_generation: NetworkGeneration(execution.upstream().network_generation()),
            };
            let caller = services
                .packet_driver_call(slot.instance_id(), &binding)
                .map_err(failure)?;
            if caller.is_none() && services.has_business_invocation() {
                return business::open_root(
                    &execution,
                    &slot,
                    tag,
                    &self.descriptor,
                    services.packet_target(),
                )
                .await;
            }
            let deadline = caller.as_ref().and_then(|caller| caller.call.deadline);
            // This is the existing local role-readiness bound, not a new total
            // lifetime on an actual caller whose original deadline is None.
            let end = deadline.unwrap_or_else(|| {
                tokio::time::Instant::now() + std::time::Duration::from_secs(30)
            });
            let role = slot.datagram(end).await.map_err(failure)?;
            let endpoint = Destination::new(
                zero_core::Address::Domain(self.descriptor.server.clone()),
                self.descriptor.port,
            );
            let mut root = NodeOpener::new(execution.upstream_observed(tag), endpoint);
            let reservation = execution.reserve_datagram().map_err(failure)?;
            let association = reservation.association();
            let (mut session, stop, handoff) = match caller {
                Some(caller) => {
                    // Same sealed facts Arc, actual record child cancellation,
                    // full binding and original driver witness. No first user,
                    // Source main or synthetic Service identity is borrowed.
                    let identity = caller.grant.identity();
                    let stop = caller.call.cancel.clone();
                    root = root
                        .with_pending(binding.clone(), Some(caller.pending.clone()))
                        .with_service_call(caller.call.clone());
                    let upstream = UpstreamTransport::new(binding.clone(), Arc::new(root));
                    caller.grant.bind_upstream(&upstream).map_err(failure)?;
                    let loan = role
                        .open_carrier(
                            DatagramRequest {
                                association,
                                targets: DatagramTargets::Multiple,
                            },
                            OpenContext {
                                identity,
                                deadline: caller.call.deadline,
                                cancel: stop.clone(),
                                upstream,
                                observer: None,
                            },
                        )
                        .await
                        .map_err(failure)?;
                    // The same Host record installs the exact LoanOwner before
                    // returning views or reporting any validation error.
                    let (session, handoff) = caller
                        .grant
                        .install_datagram_loan(loan, association)
                        .map_err(failure)?;
                    let valid = match execution.components.as_ref() {
                        Ok(plan) => plan.validate_carrier_origin(slot.role_id(), handoff.origin()),
                        Err(source) => Err(crate::component::instance::retained(source.clone())),
                    };
                    if let Err(source) = valid {
                        drop(session);
                        handoff.request_retirement();
                        return Err(failure(source));
                    }
                    (session, stop, Some(handoff))
                }
                None => {
                    // Only the explicit independent library contract uses this
                    // original branch. A Native Source must supply its grant.
                    let stop = CancellationToken::new();
                    let session = role
                        .open(
                            DatagramRequest {
                                association,
                                targets: DatagramTargets::Multiple,
                            },
                            OpenContext {
                                identity: RequestIdentity::Service(
                                    Arc::new(ServiceRequestFacts {
                                        origin: ServiceOrigin::Runtime,
                                        purpose: ServicePurpose::BackgroundControl,
                                    })
                                    .into(),
                                ),
                                deadline: Some(end),
                                cancel: stop.clone(),
                                upstream: UpstreamTransport::new(binding, Arc::new(root)),
                                observer: None,
                            },
                        )
                        .await
                        .map_err(failure)?;
                    (session, stop, None)
                }
            };
            session.control = reservation.adopt(session.control);
            Ok(Arc::new(Path::new(session, association, stop, handoff))
                as Arc<dyn PacketPathCarrier>)
        })
    }
}
