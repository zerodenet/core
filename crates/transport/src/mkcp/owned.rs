//! One mKCP original on the existing bounded packet queues and host driver book.
//! Independent library connect remains explicit; this path never spawns a task
//! outside the actual granting Scope or constructs a Quinn endpoint.
use super::memory as budget;
use super::Settings;
use crate::quic::owned::{
    packet::{BorrowedPacketSegment, PacketSegment, PacketSocket},
    DriverBody, DriverDelivery, DriverExitReceiver, OwnedDriverOrigin, OwnedEndpointLimits,
};
use std::{io, net::SocketAddr, sync::Arc};
use zero_core::component::TaskId;
use zero_platform_tokio::{
    contract::{scope::TaskRegistrar, CancellationToken, ChannelError, Impact, Reason, Stage},
    controlled_carrier::{loan::CarrierEndpointUserGraph, CarrierBufferBudget},
    controlled_datagram::metadata::users::{drivers::DriverOwner, MetadataUserGraph},
    TcpRelayStream,
};
mod graph;
mod retirement;
mod start;
mod stream;
use graph::Graph;

pub struct OwnedMkcp {
    socket: Option<PacketSocket>,
    graph: Graph,
    receiver: DriverExitReceiver,
    settings: Settings,
    buffers: CarrierBufferBudget,
    stop: CancellationToken,
    attempted: bool,
    driver: Option<TaskId>,
    masks: Vec<(TaskId, u8, CancellationToken)>,
    received: Vec<DriverDelivery>,
    _metadata: zero_traits::packet_metadata::PacketMetadataCredit,
}
impl OwnedMkcp {
    pub fn native(
        socket: PacketSocket,
        segment: &mut PacketSegment,
        tasks: TaskRegistrar,
        limits: OwnedEndpointLimits,
        settings: Settings,
        buffers: CarrierBufferBudget,
        metadata: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        settings.validate().map_err(failure)?;
        let credit = Self::metadata(limits, metadata)?;
        let (graph, receiver) = segment.take_endpoint_users(&socket, &tasks, limits)?;
        segment.bind_endpoint(&socket, graph.drivers().clone())?;
        Ok(Self::new(
            socket,
            Graph::Native(graph),
            receiver,
            settings,
            buffers,
            limits.drivers.get(),
            credit,
        ))
    }
    pub fn borrowed(
        socket: PacketSocket,
        segment: &mut BorrowedPacketSegment,
        tasks: TaskRegistrar,
        limits: OwnedEndpointLimits,
        settings: Settings,
        buffers: CarrierBufferBudget,
        metadata: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> Result<Self, ChannelError> {
        settings.validate().map_err(failure)?;
        let credit = Self::metadata(limits, metadata)?;
        let (graph, receiver) = segment.bind_endpoint_users(&socket, tasks, limits)?;
        Ok(Self::new(
            socket,
            Graph::Borrowed(graph),
            receiver,
            settings,
            buffers,
            limits.drivers.get(),
            credit,
        ))
    }
    fn new(
        socket: PacketSocket,
        graph: Graph,
        receiver: DriverExitReceiver,
        settings: Settings,
        buffers: CarrierBufferBudget,
        drivers: usize,
        metadata: zero_traits::packet_metadata::PacketMetadataCredit,
    ) -> Self {
        Self {
            socket: Some(socket),
            graph,
            receiver,
            settings,
            buffers,
            stop: CancellationToken::new(),
            attempted: false,
            driver: None,
            masks: Vec::with_capacity(drivers.saturating_sub(1)),
            received: Vec::with_capacity(drivers),
            _metadata: metadata,
        }
    }
    fn metadata(
        limits: OwnedEndpointLimits,
        metadata: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
    ) -> Result<zero_traits::packet_metadata::PacketMetadataCredit, ChannelError> {
        use zero_traits::packet_metadata::PacketMetadataLayout as Layout;
        let layout = Layout::array::<DriverDelivery>(limits.drivers.get())
            .and_then(|layout| {
                layout.checked_add(Layout::array::<(TaskId, u8, CancellationToken)>(
                    limits.drivers.get().saturating_sub(1),
                )?)
            })
            .map_err(failure)?;
        metadata.reserve(layout).map_err(failure)
    }
    pub fn borrowed_users_closed(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::loan::CarrierEndpointUsersClosed> {
        match &self.graph {
            Graph::Borrowed(graph) => graph.completion().try_owned_closed(),
            Graph::Native(_) => None,
        }
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(source)
}
