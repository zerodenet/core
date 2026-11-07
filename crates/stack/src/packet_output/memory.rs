//! Actual, finite packet transform storage. This owns only the buffers already
//! admitted for one packet, never a pool, task, native handle or IO grant.
use std::sync::Arc;
use zero_traits::{PacketBuffer, PacketDeriveError};

mod layout;
pub use layout::PacketWireMemory;
mod notifications;
pub use notifications::PacketSendNotifications;
/// Reserve every concrete transform before the original enters a queue. Actual
/// derived buffers later split that same reservation without reacquiring root
/// capacity. On failure the original payload and its credit remain unchanged.
pub fn prepare_transmit(
    mut packet: PacketBuffer,
    groups: Vec<Vec<usize>>,
) -> Result<PacketBuffer, (PacketDeriveError, PacketBuffer)> {
    let (frames, bytes) = match dimensions(&groups) {
        Ok(dimensions) => dimensions,
        Err(source) => return Err((source, packet)),
    };
    match packet.prepare_transmit(frames, bytes) {
        Ok(()) => Ok(packet),
        Err(source) => Err((source, packet)),
    }
}
/// Actual Host-owned producers reserve the original allocation and all pending
/// transforms atomically. A library-only Vec sender has no such Host binding.
pub fn allocate_transmit(
    root: &Arc<zero_tun::packet_memory::PacketReturnTotals>,
    original: usize,
    groups: Vec<Vec<usize>>,
) -> Result<PacketBuffer, PacketDeriveError> {
    let (frames, bytes) = dimensions(&groups)?;
    root.transmit_packet(original, frames, bytes)
        .map_err(memory_error)
}
fn dimensions(groups: &[Vec<usize>]) -> Result<(usize, usize), PacketDeriveError> {
    groups
        .iter()
        .flatten()
        .try_fold((0usize, 0usize), |(frames, bytes), length| {
            Ok((
                frames.checked_add(1).ok_or(PacketDeriveError::Overflow)?,
                bytes
                    .checked_add(*length)
                    .ok_or(PacketDeriveError::Overflow)?,
            ))
        })
}
pub(super) fn memory_error(
    source: zero_tun::packet_memory::PacketMemoryError,
) -> PacketDeriveError {
    use zero_tun::packet_memory::PacketMemoryError;
    match source {
        PacketMemoryError::Capacity => PacketDeriveError::Capacity,
        PacketMemoryError::Busy => PacketDeriveError::Busy,
        PacketMemoryError::Overflow => PacketDeriveError::Overflow,
        PacketMemoryError::Paused | PacketMemoryError::Stopped => PacketDeriveError::Revoked,
    }
}

#[derive(Clone)]
pub(super) struct Binding {
    origin: Origin,
    wire: PacketWireMemory,
}
#[derive(Clone)]
enum Origin {
    // This return sink only forwards the original buffer's already granted
    // storage. Pure layout metadata cannot allocate from another Root.
    ExistingStorage,
    Standalone(Arc<zero_tun::packet_memory::PacketReturnTotals>),
    Original(zero_tun::packet_memory::PacketMemoryView),
    PendingOriginal(Arc<std::sync::OnceLock<zero_tun::packet_memory::PacketMemoryView>>),
}
impl Binding {
    fn allocate(
        &self,
        length: usize,
        groups: Vec<Vec<usize>>,
    ) -> Result<PacketBuffer, PacketDeriveError> {
        match &self.origin {
            Origin::ExistingStorage => Err(PacketDeriveError::Unreserved),
            Origin::Standalone(root) => allocate_transmit(root, length, groups),
            Origin::Original(view) => {
                let (frames, bytes) = dimensions(&groups)?;
                view.transmit_packet(length, frames, bytes)
                    .map_err(memory_error)
            }
            Origin::PendingOriginal(cell) => {
                let view = cell.get().ok_or(PacketDeriveError::Unreserved)?;
                let (frames, bytes) = dimensions(&groups)?;
                view.transmit_packet(length, frames, bytes)
                    .map_err(memory_error)
            }
        }
    }
    fn check_open(&self) -> Result<(), PacketDeriveError> {
        match &self.origin {
            Origin::ExistingStorage => Ok(()),
            Origin::Standalone(_) => Ok(()),
            Origin::Original(view) => view.check_open().map_err(memory_error),
            Origin::PendingOriginal(cell) => cell
                .get()
                .ok_or(PacketDeriveError::Unreserved)?
                .check_open()
                .map_err(memory_error),
        }
    }
    pub(super) fn changed(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        let origin = self.origin.clone();
        Box::pin(async move {
            match origin {
                Origin::ExistingStorage => std::future::pending().await,
                Origin::Standalone(root) => root.notified().await,
                Origin::Original(view) => view.changed().await,
                Origin::PendingOriginal(cell) => match cell.get() {
                    Some(view) => view.changed().await,
                    None => std::future::pending().await,
                },
            }
        })
    }
}
impl std::fmt::Debug for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketMemoryBinding")
            .field("wire", &self.wire)
            .finish_non_exhaustive()
    }
}
impl super::PacketSender {
    /// A role's return sink receives only its actual Source's transform sizes.
    /// Preparing a return delegates to that return's existing storage grant;
    /// this metadata cannot allocate new packets or select an accounting Root.
    pub fn with_packet_transmit_layout(
        mut self,
        wire: PacketWireMemory,
    ) -> Result<Self, PacketDeriveError> {
        if matches!(&self.channel, super::Channel::Plain(_)) {
            return Err(PacketDeriveError::Unreserved);
        }
        self.memory = Some(Binding {
            origin: Origin::ExistingStorage,
            wire,
        });
        Ok(self)
    }
    /// The concrete existing Host passes its same Kernel root. This is buffer
    /// accounting only; it can neither bind IO nor register another worker.
    pub fn with_packet_memory(
        mut self,
        root: Arc<zero_tun::packet_memory::PacketReturnTotals>,
        wire: PacketWireMemory,
    ) -> Result<Self, PacketDeriveError> {
        if matches!(&self.channel, super::Channel::Plain(_)) {
            // A Vec-only queue cannot retain the same storage/controller
            // through the real consumer. It remains explicit library IO.
            return Err(PacketDeriveError::Unreserved);
        }
        self.memory = Some(Binding {
            origin: Origin::Standalone(root),
            wire,
        });
        Ok(self)
    }
    /// Production receives only the allocation view issued by its same
    /// original Host memory grant. It receives no Root policy setters.
    pub fn with_packet_memory_view(
        mut self,
        view: zero_tun::packet_memory::PacketMemoryView,
        wire: PacketWireMemory,
    ) -> Result<Self, PacketDeriveError> {
        if matches!(&self.channel, super::Channel::Plain(_)) {
            return Err(PacketDeriveError::Unreserved);
        }
        view.check_open().map_err(memory_error)?;
        self.memory = Some(Binding {
            origin: Origin::Original(view),
            wire,
        });
        Ok(self)
    }
    /// The existing linked software graph is built before its Source binds.
    /// This same graph's one actual Source later installs its allocation view;
    /// until then attempts fail Unreserved, never select library allocation.
    pub fn with_packet_memory_cell(
        mut self,
        cell: Arc<std::sync::OnceLock<zero_tun::packet_memory::PacketMemoryView>>,
        wire: PacketWireMemory,
    ) -> Result<Self, PacketDeriveError> {
        if matches!(&self.channel, super::Channel::Plain(_)) {
            return Err(PacketDeriveError::Unreserved);
        }
        self.memory = Some(Binding {
            origin: Origin::PendingOriginal(cell),
            wire,
        });
        Ok(self)
    }
    pub fn has_packet_memory(&self) -> bool {
        self.memory.is_some()
    }
    pub fn wire_memory(&self) -> Option<PacketWireMemory> {
        self.memory.as_ref().map(|binding| binding.wire)
    }
    /// The actual Source's allocation-only view. A pure return sink or an
    /// explicit independent library Root cannot impersonate this original.
    pub fn packet_memory_view(&self) -> Option<zero_tun::packet_memory::PacketMemoryView> {
        match &self.memory.as_ref()?.origin {
            Origin::Original(view) => Some(view.clone()),
            Origin::PendingOriginal(cell) => cell.get().cloned(),
            Origin::ExistingStorage | Origin::Standalone(_) => None,
        }
    }
    pub fn allocate_transmit(&self, length: usize) -> Result<PacketBuffer, PacketDeriveError> {
        let binding = self.memory.as_ref().ok_or(PacketDeriveError::Unreserved)?;
        let workspace = binding.wire.workspace_lengths(length)?;
        let groups = if workspace.is_empty() {
            Vec::new()
        } else {
            vec![workspace]
        };
        binding.allocate(length, groups)
    }
    pub fn allocate_transmit_groups(
        &self,
        length: usize,
        groups: Vec<Vec<usize>>,
    ) -> Result<PacketBuffer, PacketDeriveError> {
        let binding = self.memory.as_ref().ok_or(PacketDeriveError::Unreserved)?;
        binding.allocate(length, groups)
    }
}

impl super::PacketSender {
    /// Complete this return's concrete link fragmentation and codec promise
    /// before it enters that link's existing bounded FIFO. The original owner
    /// performs accounting; this sink only supplies its actual immutable sizes.
    pub fn prepare_transmit_buffer(
        &self,
        packet: &mut PacketBuffer,
        mtu: usize,
    ) -> Result<(), PacketDeriveError> {
        let binding = self.memory.as_ref().ok_or(PacketDeriveError::Unreserved)?;
        binding.check_open()?;
        prepare_transmit_layout(packet, mtu, binding.wire)
    }
    pub fn transmit_memory_changed(
        &self,
    ) -> Result<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>, PacketDeriveError>
    {
        self.memory
            .as_ref()
            .map(Binding::changed)
            .ok_or(PacketDeriveError::Unreserved)
    }
}

/// Existing packet custody admits its whole exact return transform before
/// queueing. Pure codec metadata neither selects a Root nor grants IO.
pub fn prepare_transmit_layout(
    packet: &mut PacketBuffer,
    mtu: usize,
    wire: PacketWireMemory,
) -> Result<(), PacketDeriveError> {
    let fragments = crate::packet::fragment_packet_lengths(packet, mtu)?;
    let lengths = if fragments.is_empty() {
        vec![packet.len()]
    } else {
        fragments.clone()
    };
    let mut groups = vec![fragments];
    for length in lengths {
        groups.push(wire.workspace_lengths(length)?);
    }
    let (frames, bytes) = dimensions(&groups)?;
    packet.prepare_transmit(frames, bytes)
}
