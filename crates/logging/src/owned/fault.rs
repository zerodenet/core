//! A prepaid actual IO source. It never references a controller or Scope.
use super::*;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoPhase {
    Write,
    Flush,
    Rotate,
    Open,
    Close,
}
#[derive(Clone)]
pub struct IoFailure(PacketMetadataArc<IoSource>);
struct IoSource {
    // A failed write is not replayed; its exact admitted bytes remain paid.
    _frame: Option<queue::Frame>,
    origin: std::sync::Weak<book::Book>,
    accepted: std::sync::atomic::AtomicBool,
    output: usize,
    phase: IoPhase,
    source: io::Error,
}
impl std::fmt::Debug for IoFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogIoFailure")
            .field("output", &self.0.output)
            .field("phase", &self.0.phase)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for IoFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "logging output {} {:?}: {}",
            self.0.output, self.0.phase, self.0.source
        )
    }
}
impl std::error::Error for IoFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0.source)
    }
}
pub(super) struct Slot {
    credit: book::Storage,
    origin: std::sync::Weak<book::Book>,
}
impl Slot {
    fn layout() -> io::Result<PacketMetadataLayout> {
        PacketMetadataArc::<IoSource>::layout()
            .and_then(|layout| {
                layout
                    .checked_add(zero_platform_tokio::file::OriginalFile::retained_header_layout()?)
            })
            // An escaped source retains the original weak book header and
            // the actual credit relay even after its controller was dropped.
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<book::Book>()?))
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<wake::Wake>()?))
            .map_err(io::Error::other)
    }
    pub(super) fn reserve(book: &Arc<book::Book>) -> io::Result<Self> {
        Ok(Self {
            credit: book.layout(Self::layout()?)?,
            origin: Arc::downgrade(book),
        })
    }
    pub(super) fn receive(
        self,
        output: usize,
        phase: IoPhase,
        error: io::Error,
        frame: Option<queue::Frame>,
    ) -> IoFailure {
        match PacketMetadataArc::from_prepaid(
            IoSource {
                _frame: frame,
                origin: self.origin,
                accepted: false.into(),
                output,
                phase,
                source: error,
            },
            self.credit,
        ) {
            Ok(source) => IoFailure(source),
            Err(_) => unreachable!("same prepaid concrete source layout"),
        }
    }
}

impl std::fmt::Debug for IoSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogIoSource")
            .field("output", &self.output)
            .field("phase", &self.phase)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for IoSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "logging output {} {:?}: {}",
            self.output, self.phase, self.source
        )
    }
}
impl std::error::Error for IoSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl IoFailure {
    /// Borrow the exact admitted failed chunk; this never copies or releases it.
    pub fn failed_bytes(&self) -> Option<&[u8]> {
        self.0._frame.as_ref().map(|frame| frame.bytes.as_ref())
    }
    pub(super) fn diagnostic(&self) -> zero_traits::packet_metadata::PacketMetadataError {
        self.0.clone().into_error()
    }
}

impl IoFailure {
    pub(super) fn same(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn matches_book(&self, book: &Arc<book::Book>) -> bool {
        self.0.origin.ptr_eq(&Arc::downgrade(book))
    }
    pub(super) fn accept(&self) {
        self.0
            .accepted
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(super) fn accepted(&self) -> bool {
        self.0.accepted.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn output(&self) -> usize {
        self.0.output
    }
}

mod worker;
pub use worker::WorkerFailure;
pub(super) use worker::WorkerSlot;
