//! Bind only the exact endpoint socket and its same unique packet queues.
use super::*;
impl OwnedEndpointOwner {
    pub fn bind_packet_segment(
        &self,
        segment: &super::super::packet::PacketSegment,
    ) -> Result<(), ChannelError> {
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        if self.shared.phase.load(Ordering::Acquire) != 0 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        segment.bind_endpoint(socket, self.drivers.registration_ledger())
    }
}

impl OwnedEndpointOwner {
    /// This client's unique packet segment is retiring for good. Keep the
    /// original wire running to drain its finite send queue after the real
    /// Quinn drivers join. RFC 9000 §10.2 permits early retirement when late
    /// packets cannot cause responses on the retiring endpoint.
    pub async fn retire_packet_client_original(&self) -> Result<(), ChannelError> {
        if !matches!(&self.native, Native::Packet(_)) || self.users_graph.is_none() {
            return Err(error(Stage::Close, Reason::StalePlan));
        }
        self.revoke_users();
        self.shared.users.wait_empty_original().await?;
        let endpoint = self.shared.endpoint.lock().unwrap().take();
        if let Some(endpoint) = endpoint {
            endpoint.retire_socket(0u32.into(), b"owner retirement");
        }
        Ok(())
    }
}

impl OwnedEndpointOwner {
    /// The native packet view has reached this exact live Quinn endpoint
    /// driver. Authentication and logical caller delivery remain separate.
    pub fn prepare_native_endpoint_receiver(
        &self,
        segment: &super::super::packet::PacketSegment,
        origin: &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        retirement: &zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement,
    ) -> Result<zero_platform_tokio::controlled_carrier::ProtocolReceiverPrepared, ChannelError>
    {
        self.shared.check()?;
        if self.shared.phase.load(Ordering::Acquire) != 2 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let id = self
            .drivers
            .tasks()
            .into_iter()
            .find(|id| {
                self.drivers.origin(*id)
                    == Some(super::super::OwnedDriverOrigin::Quinn(
                        quinn::DriverOrigin::Endpoint,
                    ))
            })
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        let graph = self
            .users_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let views = segment
            .retained_receiver_views(socket, graph, id)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        origin.prepare_protocol_receiver(views, retirement)
    }

    pub fn new_packet_endpoint(
        socket: super::super::packet::PacketSocket,
        segment: &mut super::super::packet::PacketSegment,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> Result<(Self, DriverExitReceiver), ChannelError> {
        let (graph, receiver) = segment.take_endpoint_users(&socket, &registrar, limits)?;
        let drivers = QuinnDriverOwner::from_ledger(graph.drivers().clone());
        let users = graph.users().clone();
        let (owner, receiver) = Self::construct_users(
            Native::Packet(socket),
            drivers,
            users,
            receiver,
            Some(graph),
        );
        owner.bind_packet_segment(segment)?;
        Ok((owner, receiver))
    }
}

impl OwnedEndpointOwner {
    pub fn prepare_native_endpoint_continuation_receiver(
        &self,
        segment: &super::super::packet::PacketSegment,
        original: &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        wire: &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        retirement: &zero_platform_tokio::controlled_carrier::CarrierProtocolRetirement,
    ) -> Result<zero_platform_tokio::controlled_carrier::ProtocolReceiverPrepared, ChannelError>
    {
        self.shared.check()?;
        if self.shared.phase.load(Ordering::Acquire) != 2 {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let previous = original
            .retained_driver()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let id = self
            .drivers
            .tasks()
            .into_iter()
            .find(|id| {
                self.drivers.origin(*id)
                    == Some(super::super::OwnedDriverOrigin::Quinn(
                        quinn::DriverOrigin::Endpoint,
                    ))
            })
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        let graph = self
            .users_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let views = segment
            .retained_continuation_views(socket, graph, id, previous.view_handoff())
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        original.prepare_protocol_continuation_receiver(wire, views, retirement)
    }
    /// Complete the first wire's receipt with this same endpoint's actual final
    /// driver map. Returning a later wire never substitutes for that first claim.
    pub fn complete_packet_protocol_receipt(
        &self,
        receipt: zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
    ) -> Result<
        zero_platform_tokio::controlled_datagram::metadata::users::MetadataUsersClosedReceipt,
        ChannelError,
    > {
        let graph = self
            .users_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Close, Reason::StalePlan))?;
        if !graph.receipt_names_original(&receipt) {
            return Err(error(Stage::Close, Reason::StalePlan));
        }
        let drivers = graph
            .drivers()
            .owned_registration_completion()
            .ok_or_else(|| error(Stage::Close, Reason::Timeout))?;
        receipt
            .with_related_driver_receipt(drivers)
            .map_err(|_| error(Stage::Close, Reason::StalePlan))
    }
}
