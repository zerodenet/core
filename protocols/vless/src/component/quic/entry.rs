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
    pub metadata_memory: PacketDriverMetadataView,
    pub resolver: Option<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>,
    pub entries: Mutex<Table>,
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
    generation: PacketMetadataArc<Generation>,
    ordinal: u64,
    index: usize,
    published: bool,
}
impl Table {
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
        let mut previous = 0;
        while let Some((ordinal, entry)) = self.next_entry(previous) {
            previous = ordinal;
            if entry.retirement.is_settled() {
                let removed = {
                    let mut table = self.entries.lock().unwrap();
                    let row = table
                        .rows
                        .iter_mut()
                        .find(|row| row.ordinal == ordinal && row.reserved)
                        .expect("same actual entry row");
                    let removed = row.entry.take();
                    if removed.is_some() {
                        row.reserved = false;
                        table.used -= 1;
                    }
                    removed
                };
                drop(removed);
            }
        }
    }
    pub fn reserve(generation: &PacketMetadataArc<Self>) -> Result<Position, ChannelError> {
        generation.reap();
        let mut table = generation.entries.lock().unwrap();
        if generation.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if table.used >= generation.options.origins.get() {
            return Err(error(Stage::Prepare, Reason::Capacity));
        }
        table.next = table
            .next
            .checked_add(1)
            .ok_or_else(|| error(Stage::Prepare, Reason::Capacity))?;
        table.used += 1;
        let ordinal = table.next;
        let (index, row) = table
            .rows
            .iter_mut()
            .enumerate()
            .find(|(_, row)| !row.reserved)
            .expect("same finite entry admission");
        row.reserved = true;
        row.ordinal = ordinal;
        Ok(Position {
            generation: generation.clone(),
            ordinal,
            index,
            published: false,
        })
    }
    pub fn next_entry(&self, previous: u64) -> Option<(u64, PacketMetadataArc<Entry>)> {
        self.entries
            .lock()
            .unwrap()
            .rows
            .iter()
            .filter(|row| row.reserved && row.ordinal > previous && row.entry.is_some())
            .min_by_key(|row| row.ordinal)
            .map(|row| (row.ordinal, row.entry.as_ref().unwrap().clone()))
    }
    pub fn request_retirement(&self) {
        let mut previous = 0;
        while let Some((ordinal, entry)) = self.next_entry(previous) {
            previous = ordinal;
            entry.retirement.request_retirement();
        }
    }
}
impl Position {
    pub fn publish(mut self, entry: PacketMetadataArc<Entry>) -> PacketMetadataArc<Entry> {
        let previous = {
            let mut table = self.generation.entries.lock().unwrap();
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
            let mut table = self.generation.entries.lock().unwrap();
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
