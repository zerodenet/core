//! Each actual acquisition prepays its finite four independent error phases.
//! Successful acquisitions do not append history. Failed originals retain the
//! exact IO causes in this same Root's paid nodes, including secondary faults.
use crate::packet_memory::{
    PacketDriverMetadataView, PacketMemoryError, PacketMetadataArc, PacketMetadataCredit,
    PacketMetadataLayout,
};
use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock,
    },
};
use zero_traits::packet_metadata::PacketMetadataError;

#[derive(Clone)]
pub struct RouteFileFaults(PacketMetadataArc<Payload>);
struct Payload {
    head: Mutex<Option<PacketMetadataArc<Attempt>>>,
    first: OnceLock<PacketMetadataArc<Attempt>>,
    stamp: Mutex<PacketMetadataCredit>,
}
pub(in crate::route) struct Attempt {
    sources: [OnceLock<io::Error>; 4],
    archived: AtomicBool,
    next: Mutex<Option<PacketMetadataArc<Attempt>>>,
}
impl RouteFileFaults {
    pub fn metadata_layout() -> Result<PacketMetadataLayout, PacketMemoryError> {
        PacketMetadataArc::<Payload>::layout()
    }
    /// Consumes the existing Source layout split, without reserving twice.
    /// Failure returns the very same original credit, before any allocation.
    pub fn from_prepaid(
        mut credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, PacketMetadataCredit)> {
        let layout = match Self::metadata_layout() {
            Ok(layout) => layout,
            Err(e) => return Err((e, credit)),
        };
        if credit.layout().allocations() < layout.allocations()
            || credit.layout().bytes() < layout.bytes()
        {
            return Err((PacketMemoryError::Capacity, credit));
        }
        // Cold constructor: split clones the original Waker outside every
        // Native/provider mutex. The zero stamp carries counter identity only.
        let stamp = match credit.split(PacketMetadataLayout::new(0, 0)) {
            Ok(stamp) => stamp,
            Err(e) => return Err((e, credit)),
        };
        let payload = Payload {
            head: Mutex::new(None),
            first: OnceLock::new(),
            stamp: Mutex::new(stamp),
        };
        match PacketMetadataArc::from_prepaid(payload, credit) {
            Ok(payload) => Ok(Self(payload)),
            Err((e, payload, credit)) => {
                drop(payload);
                Err((e, credit))
            }
        }
    }
    /// Genuine independent convenience, not an alternate Kernel budget.
    pub fn prepare(memory: &PacketDriverMetadataView) -> Result<Self, PacketMemoryError> {
        let credit = memory.reserve(Self::metadata_layout()?)?;
        Self::from_prepaid(credit).map_err(|(e, _)| e)
    }
    pub fn diagnostic(&self) -> PacketMetadataError {
        self.0.clone().into_error()
    }
    pub fn has_failures(&self) -> bool {
        self.0
            .head
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }
    pub(in crate::route) fn begin(
        &self,
        memory: &PacketDriverMetadataView,
    ) -> Result<PacketMetadataArc<Attempt>, PacketMemoryError> {
        // Admission/pause is required only for NEW acquisition, never close.
        {
            let mut stamp = self.0.stamp.lock().unwrap_or_else(|e| e.into_inner());
            memory.grow_credit(&mut stamp, PacketMetadataLayout::new(0, 0))?;
        }
        let credit = memory.reserve(PacketMetadataArc::<Attempt>::layout()?)?;
        PacketMetadataArc::from_prepaid(
            Attempt {
                sources: std::array::from_fn(|_| OnceLock::new()),
                archived: AtomicBool::new(false),
                next: Mutex::new(None),
            },
            credit,
        )
        .map_err(|(e, _, _)| e)
    }
    pub(in crate::route) fn retain(
        &self,
        attempt: &PacketMetadataArc<Attempt>,
        phase: usize,
        source: io::Error,
    ) {
        // A phase belongs to ONE cached syscall/outcome; no repeating unknown
        // syscall can manufacture an unbounded number of causes in this cell.
        assert!(attempt.sources[phase].set(source).is_ok());
        // The original first source stays directly borrowable through Error;
        // all subsequent independent causes remain in this same paid delivery.
        let _ = self.0.first.set(attempt.clone());
        if !attempt.archived.swap(true, Ordering::AcqRel) {
            let mut head = self.0.head.lock().unwrap_or_else(|e| e.into_inner());
            *attempt.next.lock().unwrap_or_else(|e| e.into_inner()) = head.take();
            *head = Some(attempt.clone());
        }
    }
}
impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut current = self.head.lock().unwrap_or_else(|e| e.into_inner()).clone();
        while let Some(node) = current {
            // No user formatter runs while a table/list mutex is held.
            for (phase, source) in node
                .sources
                .iter()
                .enumerate()
                .filter_map(|(p, s)| s.get().map(|s| (p, s)))
            {
                write!(f, "route File phase {phase}: {source}; ")?;
            }
            current = node.next.lock().unwrap_or_else(|e| e.into_inner()).clone();
        }
        Ok(())
    }
}
impl std::error::Error for Payload {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.first
            .get()
            .and_then(|first| first.sources.iter().find_map(OnceLock::get))
            .map(|source| source as _)
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        let mut current = self
            .head
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        while let Some(mut node) = current {
            if let Some(node) = PacketMetadataArc::get_mut(&mut node) {
                current = node
                    .next
                    .get_mut()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
            }
            // Nodes still retained by an actual original stay paid; no cycles.
        }
    }
}

impl fmt::Debug for RouteFileFaults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RouteFileFaults")
    }
}
impl fmt::Debug for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RouteFileAttempt")
    }
}
