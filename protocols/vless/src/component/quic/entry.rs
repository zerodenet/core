//! One finite entry table covers native, borrowed, pending and retiring calls.
use super::*;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::{network::UpstreamTransport, CancellationToken, RetainedError},
    controlled_carrier::{CarrierGrants, CarrierOrigin, CarrierProtocolRetirement},
    controlled_datagram::metadata_pool::MetadataPoolOwner,
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit};
pub(super) mod actor;
mod cleanup;
mod control;
pub(super) mod diagnostics;
mod table;
pub(super) use control::{LogicalControl, ProtocolControl, View};
pub(super) struct Generation {
    pub instance: ComponentInstanceId,
    pub options: Arc<Options>,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub upstream: UpstreamTransport,
    pub metadata: MetadataPoolOwner,
    pub carrier: CarrierGrants,
    pub cancel: CancellationToken,
    pub cleanup: CancellationToken,
    pub buffers: zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    pub masks: zero_transport::finalmask::Profile,
    pub metadata_memory: PacketDriverMetadataView,
    pub resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    pub entries: PacketMetadataArc<Mutex<Table>>,
}
pub(super) struct Table {
    next: u64,
    used: usize,
    rows: Vec<Row>,
    _memory: PacketMetadataCredit,
}
pub(super) struct Row {
    ordinal: u64,
    reserved: bool,
    entry: Option<PacketMetadataArc<Entry>>,
}
pub(super) struct Position {
    entries: PacketMetadataArc<Mutex<Table>>,
    ordinal: u64,
    index: usize,
    published: bool,
}
impl Table {
    /// Exact missing-original claims from the selected upload/download groups.
    /// This only claims the existing local Entry positions: each real Root
    /// controller/grant must still be installed before either initializing IO.
    pub fn reserve_needed(
        entries: &PacketMetadataArc<Mutex<Self>>,
        needed: [bool; 2],
    ) -> Result<[Option<Position>; 2], ChannelError> {
        let count = needed.into_iter().filter(|needed| *needed).count();
        let mut table = entries.lock().unwrap();
        let next = table
            .next
            .checked_add(count as u64)
            .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?;
        let used = table
            .used
            .checked_add(count)
            .filter(|used| *used <= table.rows.len())
            .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?;
        let mut ordinal = table.next;
        // All arithmetic and capacity checks precede the first row mutation.
        let positions = needed.map(|needed| {
            if !needed {
                return None;
            }
            let (index, row) = table
                .rows
                .iter_mut()
                .enumerate()
                .find(|(_, row)| !row.reserved)
                .expect("same actual shared Entry admission");
            ordinal += 1;
            row.reserved = true;
            row.ordinal = ordinal;
            Some(Position {
                entries: entries.clone(),
                ordinal,
                index,
                published: false,
            })
        });
        table.next = next;
        table.used = used;
        Ok(positions)
    }
    pub fn shared(
        memory: &PacketDriverMetadataView,
        limit: usize,
    ) -> Result<PacketMetadataArc<Mutex<Self>>, ChannelError> {
        let credit = memory
            .reserve(
                PacketMetadataArc::<Mutex<Self>>::layout().map_err(|source| {
                    error(Stage::Prepare, Reason::Capacity).with_source(source)
                })?,
            )
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let table = Self::new(memory, limit)?;
        PacketMetadataArc::from_prepaid(Mutex::new(table), credit)
            .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))
    }
    pub fn new(memory: &PacketDriverMetadataView, limit: usize) -> Result<Self, ChannelError> {
        let credit = memory
            .reserve(
                zero_traits::packet_metadata::PacketMetadataLayout::array::<Row>(limit).map_err(
                    |source| error(Stage::Prepare, Reason::Capacity).with_source(source),
                )?,
            )
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(limit)
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        rows.resize_with(limit, || Row {
            ordinal: 0,
            reserved: false,
            entry: None,
        });
        Ok(Self {
            next: 0,
            used: 0,
            rows,
            _memory: credit,
        })
    }
}
pub(super) struct Entry {
    pub owners: PacketMetadataArc<Owners>,
    pub retirement: CarrierProtocolRetirement,
    pub origin: std::sync::OnceLock<CarrierOrigin>,
}
pub(super) struct Owners {
    pub state: AsyncMutex<State>,
    pub stop: CancellationToken,
    pub changed: Notify,
    pub returned: std::sync::atomic::AtomicBool,
    pub tasks: TaskRegistrar,
    actor: Mutex<actor::Registration>,
    actor_exit: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoined>>,
    actor_changed: Notify,
    pub diagnostics: diagnostics::Diagnostics,
    pub protocols:
        std::sync::OnceLock<zero_platform_tokio::controlled_carrier::ProtocolOriginalChildren>,
}
#[derive(Default)]
pub(super) struct State {
    pub tcp_masks: Option<zero_transport::finalmask::tcp::OwnedMasks>,
    pub tcp_setup: Option<super::mixed::tcp::setup::Setup>,
    pub tcp: Option<zero_transport::split_http::PreparedXhttpTcp>,
    pub tcp_driver: Option<zero_platform_tokio::contract::scope::TaskJoinWitness>,
    pub tcp_joined: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    pub endpoint: Option<zero_transport::quic::owned::OwnedEndpointOwner>,
    pub receiver: Option<zero_transport::quic::owned::DriverExitReceiver>,
    pub segment: Option<Segment>,
    pub queues: Option<zero_transport::quic::owned::packet::PacketQueues>,
    pub unbound: Option<GrantedMetadataListener>,
    pub native_origin:
        Option<zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin>,
    pub retry_connection: bool,
    pub logical_memory: Option<PacketMetadataCredit>,
    pub h3: Option<zero_transport::split_http::PreparedXhttp3>,
    pub cleanup_complete: bool,
    pub received: Vec<zero_transport::quic::owned::DriverDelivery>,
    pub _received_memory: Option<PacketMetadataCredit>,
    pub borrowed_joined: Option<zero_platform_tokio::contract::scope::TaskJoined>,
}
pub(super) enum Segment {
    Native(zero_transport::quic::owned::packet::PacketSegment),
    Borrowed(zero_transport::quic::owned::packet::BorrowedPacketSegment),
}
impl Generation {
    pub fn reap(&self) {
        Table::reap(&self.entries);
    }
    pub fn reserve(generation: &PacketMetadataArc<Self>) -> Result<Position, ChannelError> {
        generation.reap();
        if generation.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let [position, _] = Table::reserve_needed(&generation.entries, [true, false])?;
        Ok(position.expect("one actual requested position"))
    }
    pub fn next_entry(&self, previous: u64) -> Option<(u64, PacketMetadataArc<Entry>)> {
        Table::next_entry(&self.entries, previous)
    }
    pub fn request_retirement(&self) {
        Table::request_retirement(&self.entries);
    }
}
impl Position {
    pub fn publish(mut self, entry: PacketMetadataArc<Entry>) -> PacketMetadataArc<Entry> {
        let previous = {
            let mut table = self.entries.lock().unwrap();
            let row = &mut table.rows[self.index];
            assert!(row.reserved && row.ordinal == self.ordinal);
            row.entry.replace(entry.clone())
        };
        assert!(previous.is_none(), "one real original entry");
        self.published = true;
        entry
    }
}
impl Drop for Position {
    fn drop(&mut self) {
        if !self.published {
            let mut table = self.entries.lock().unwrap();
            let row = &mut table.rows[self.index];
            assert!(row.reserved && row.ordinal == self.ordinal && row.entry.is_none());
            row.reserved = false;
            table.used -= 1;
        }
    }
}
impl Owners {
    pub fn new(
        tasks: TaskRegistrar,
        diagnostics: diagnostics::Diagnostics,
        received_memory: PacketMetadataCredit,
        logical_memory: PacketMetadataCredit,
        drivers: usize,
    ) -> Self {
        Self {
            tasks,
            stop: CancellationToken::new(),
            state: AsyncMutex::new(State {
                logical_memory: Some(logical_memory),
                _received_memory: Some(received_memory),
                received: Vec::with_capacity(drivers),
                ..Default::default()
            }),
            changed: Notify::new(),
            returned: false.into(),
            actor: Mutex::new(actor::Registration::Pending),
            actor_exit: Mutex::new(None),
            actor_changed: Notify::new(),
            diagnostics,
            protocols: std::sync::OnceLock::new(),
        }
    }
    pub fn receive_setup(&self, source: ChannelError) -> RetainedError {
        self.diagnostics.operation(source)
    }
}
