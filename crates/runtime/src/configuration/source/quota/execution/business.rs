//! Known-close write errors are business status, not fabricated task failures.
use super::*;
use zero_traits::packet_metadata::PacketMetadataArc;

pub(super) struct Cell(Option<QuotaMemoryCredit>);
struct Failure {
    original: io::Error,
}
#[derive(Clone)]
struct Delivered(PacketMetadataArc<Failure>);
impl Cell {
    pub(super) fn reserve(memory: &QuotaMemoryBudget) -> io::Result<Self> {
        // Same existing io::Error/Arc facade is not an exact-RSS promise for
        // opaque std wrappers. This layout covers our concrete source/header
        // allocation and keeps it attached through every borrowed diagnostic.
        let layout = PacketMetadataArc::<Failure>::layout().map_err(io::Error::other)?;
        memory.reserve(layout).map(|credit| Self(Some(credit)))
    }
    pub(super) fn receive(mut self, original: io::Error) -> Arc<io::Error> {
        let kind = original.kind();
        let failure = PacketMetadataArc::from_prepaid(Failure { original }, self.0.take().unwrap().into_metadata());
        let failure = match failure { Ok(failure) => failure, Err(_) => unreachable!("same prepaid quota business failure layout") };
        // Every clone/extracted inner error keeps the attachment alive. There
        // is no task witness, joined/Scope marker or mutable receiver back edge.
        Arc::new(io::Error::new(kind, Delivered(failure)))
    }
}
impl std::fmt::Debug for Delivered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_tuple("QuotaWriteFailure").field(&self.0.original.kind()).finish() }
}
impl std::fmt::Display for Delivered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.original.fmt(f) }
}
impl std::error::Error for Delivered {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(&self.0.original) }
}
