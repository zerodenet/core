//! Exact original task failure; no fabricated ID, task result or Scope ACK.
use super::*;
use zero_platform_tokio::contract::scope::{TaskJoinWitness, TaskJoined};
#[derive(Clone)]
pub struct WorkerFailure(PacketMetadataArc<WorkerSource>);
struct WorkerSource {
    // Same abandoned working bytes, now a failed data result with no IO grant.
    _frame: Option<queue::Frame>,
    joined: TaskJoined,
    source: Arc<zero_platform_tokio::contract::ChannelError>,
    _headers: PacketMetadataArc<Headers>,
}
impl std::fmt::Debug for WorkerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggingWorkerFailure")
            .field("task", &self.0.joined.id())
            .field("class", &self.0.joined.class())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "logging worker {:?}: {}",
            self.0.joined.id(),
            self.0.source
        )
    }
}
impl std::error::Error for WorkerFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.source.as_ref())
    }
}
// Pure header promise, with no witness/Scope/controller back edge.
struct Headers {
    _storage: book::Storage,
}
pub(in crate::owned) struct WorkerSlot {
    storage: book::Storage,
    headers: PacketMetadataArc<Headers>,
}
impl WorkerSlot {
    pub(in crate::owned) fn reserve(book: &Arc<book::Book>) -> io::Result<Self> {
        let source_layout =
            PacketMetadataArc::<WorkerSource>::layout().map_err(io::Error::other)?;
        let header_layout = TaskJoinWitness::task_failure_header_layout()
            .and_then(|n| n.checked_add(PacketMetadataLayout::arc::<wake::Wake>()?))
            .map_err(io::Error::other)?;
        let guard_layout = PacketMetadataArc::<Headers>::layout().map_err(io::Error::other)?;
        let total = source_layout
            .checked_add(header_layout)
            .and_then(|n| n.checked_add(guard_layout))
            .map_err(io::Error::other)?;
        // One admission: a rejected job cannot partially refund and self-wake
        // while retrying that same impossible set of header promises.
        let mut source = book.layout(total)?;
        let header_storage = source.split(header_layout).map_err(io::Error::other)?;
        let guard_storage = source.split(guard_layout).map_err(io::Error::other)?;
        let headers = PacketMetadataArc::from_prepaid(
            Headers {
                _storage: header_storage,
            },
            guard_storage,
        )
        .map_err(|(source, _, _)| io::Error::other(source))?;
        Ok(Self {
            storage: source,
            headers,
        })
    }

    pub(in crate::owned) fn receive(
        self,
        witness: &TaskJoinWitness,
        frame: Option<queue::Frame>,
    ) -> io::Result<Option<WorkerFailure>> {
        let joined = witness.try_joined().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "original logging worker is not joined",
            )
        })?;
        let source = match joined.result() {
            Ok(()) => return Ok(None),
            Err(source) => source,
        };
        if !witness.matches_joined(&joined) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign logging worker receipt",
            ));
        }
        let source = match PacketMetadataArc::from_prepaid(
            WorkerSource {
                _frame: frame,
                joined,
                source,
                _headers: self.headers,
            },
            self.storage,
        ) {
            Ok(source) => source,
            Err(_) => unreachable!("same prepaid original task failure layout"),
        };
        Ok(Some(WorkerFailure(source)))
    }
}
impl WorkerFailure {
    pub fn failed_bytes(&self) -> Option<&[u8]> {
        self.0._frame.as_ref().map(|frame| frame.bytes.as_ref())
    }
    pub(in crate::owned) fn same(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
    pub(in crate::owned) fn joined(&self) -> &TaskJoined {
        &self.0.joined
    }
}
