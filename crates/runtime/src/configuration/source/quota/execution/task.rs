//! Prepaid actual task headers. Body return and Scope ACK are not header GC.
use super::*;
use zero_platform_tokio::contract::{
    scope::{HostedExitReceipt, TaskJoinWitness, TaskJoined},
    ChannelError,
};
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataLayout};

struct Headers {
    _liability: QuotaMemoryCredit,
}
pub(super) struct Slot {
    source: Option<QuotaMemoryCredit>,
    headers: PacketMetadataArc<Headers>,
}
#[derive(Clone)]
pub struct QuotaWriterFailure(PacketMetadataArc<Failure>);
struct Failure {
    joined: TaskJoined,
    original: Arc<ChannelError>,
    _headers: PacketMetadataArc<Headers>,
}
impl Slot {
    pub(super) fn reserve(memory: &QuotaMemoryBudget) -> io::Result<Self> {
        let source = PacketMetadataArc::<Failure>::layout().map_err(io::Error::other)?;
        let headers = PacketMetadataArc::<Headers>::layout().map_err(io::Error::other)?;
        let liability = TaskJoinWitness::task_failure_header_layout()
            .and_then(|layout| layout.checked_add(HostedExitReceipt::offer_header_layout()?))
            .map_err(io::Error::other)?;
        let liability = liability
            .checked_add(PacketMetadataLayout::arc::<super::Gate>().map_err(io::Error::other)?)
            .and_then(|layout| {
                layout.checked_add(PacketMetadataLayout::arc::<super::standard::Signal>()?)
            })
            .map_err(io::Error::other)?;
        let layout = source
            .checked_add(headers)
            .and_then(|layout| layout.checked_add(liability))
            .map_err(io::Error::other)?;
        // The initial Source storage prepares this before decode or IO, so a
        // full map/snapshot promise never vetoes its own final-flush actor.
        let mut credit = memory.reserve(layout)?;
        let header = credit.split(headers)?;
        let liability = credit.split(liability)?;
        let headers = match PacketMetadataArc::from_prepaid(
            Headers {
                _liability: liability,
            },
            header.into_metadata(),
        ) {
            Ok(value) => value,
            Err((error, value, credit)) => {
                drop(value);
                drop(credit);
                return Err(io::Error::other(error));
            }
        };
        Ok(Self {
            source: Some(credit),
            headers,
        })
    }
    pub(super) fn receive(
        &mut self,
        witness: &TaskJoinWitness,
    ) -> io::Result<Option<QuotaWriterFailure>> {
        let joined = witness.try_joined().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "actual quota actor remains unjoined",
            )
        })?;
        if !witness.matches_joined(&joined) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign quota actor result",
            ));
        }
        let original = match joined.result() {
            Ok(()) => return Ok(None),
            Err(source) => source,
        };
        let credit = self
            .source
            .take()
            .ok_or_else(|| io::Error::other("quota actor failure was already delivered"))?;
        let value = Failure {
            joined,
            original,
            _headers: self.headers.clone(),
        };
        let value = match PacketMetadataArc::from_prepaid(value, credit.into_metadata()) {
            Ok(value) => value,
            Err((error, value, credit)) => {
                drop(value);
                drop(credit);
                return Err(io::Error::other(error));
            }
        };
        Ok(Some(QuotaWriterFailure(value)))
    }
}
impl QuotaWriterFailure {
    pub(super) fn joined(&self) -> &TaskJoined {
        &self.0.joined
    }
    pub fn task_id(&self) -> zero_core::component::TaskId {
        self.0.joined.id()
    }
    pub fn native_task_id_matches(&self, witness: &TaskJoinWitness) -> bool {
        witness.matches_joined(&self.0.joined)
    }
    pub fn class(&self) -> zero_platform_tokio::contract::lifecycle::TaskClass {
        self.0.joined.class()
    }
    pub fn same_failure(&self, other: &Self) -> bool {
        PacketMetadataArc::ptr_eq(&self.0, &other.0)
    }
}
impl std::fmt::Debug for QuotaWriterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuotaWriterFailure")
            .field("task", &self.task_id())
            .field("class", &self.class())
            .finish()
    }
}
impl std::fmt::Display for QuotaWriterFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.original.fmt(f)
    }
}
impl std::error::Error for QuotaWriterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.original.as_ref())
    }
}
