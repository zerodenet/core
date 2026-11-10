//! Prepay the concrete serde reader/decoded envelope, preserving its decoder.
use super::*;
use std::io::{Read, Seek};
use zero_runtime::configuration::{QuotaMemoryBudget, QuotaMemoryCredit};
use zero_traits::packet_metadata::PacketMetadataLayout;
mod facts;
use facts::{Facts, Guarded};

pub(super) struct Decoded {
    pub value: PersistedQuotaBook,
    // Data is destroyed before its allocation promise can return.
    pub credit: QuotaMemoryCredit,
}
#[derive(Debug)]
struct FailedDecode {
    original: io::Error,
    _credit: QuotaMemoryCredit,
}
impl std::fmt::Display for FailedDecode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.original.fmt(f)
    }
}
impl std::error::Error for FailedDecode {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.original)
    }
}
fn overflow() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "quota decoding layout overflow")
}
fn grown_capacity(minimum: usize) -> io::Result<usize> {
    minimum
        .max(8)
        .checked_next_power_of_two()
        .ok_or_else(overflow)
}
fn add(a: PacketMetadataLayout, b: PacketMetadataLayout) -> io::Result<PacketMetadataLayout> {
    a.checked_add(b).map_err(|_| overflow())
}
impl Facts {
    fn envelope(self) -> io::Result<PacketMetadataLayout> {
        // serde's IO string/number scratch grows geometrically from 8 bytes;
        // prepay current+old allocation at its real largest reallocation. Raw
        // token bytes dominate UTF-8 unescaping; insignificant whitespace does
        // not count toward this dimension (including the 256MiB oracle).
        let scratch = grown_capacity(self.token)?;
        let scratch_peak = scratch.checked_add(scratch / 2).ok_or_else(overflow)?;
        let mut layout = PacketMetadataLayout::new(2, scratch_peak);
        layout = add(
            layout,
            PacketMetadataLayout::array::<u8>(8192).map_err(|_| overflow())?,
        )?;
        // Every valid row has an object; malformed/unknown input may count more
        // but remains accepted/rejected only by the unchanged typed decoder.
        let rows = grown_capacity(self.objects)?;
        layout = add(
            layout,
            PacketMetadataLayout::array::<PersistedQuotaBalance>(rows).map_err(|_| overflow())?,
        )?;
        layout = add(
            layout,
            PacketMetadataLayout::array::<PersistedQuotaBalance>(rows / 2)
                .map_err(|_| overflow())?,
        )?;
        // Deserialize String copies only its decoded scratch contents. Sum of
        // all raw quoted token lengths conservatively covers every such copy.
        layout = add(
            layout,
            PacketMetadataLayout::new(
                self.objects.checked_add(1).ok_or_else(overflow)?,
                self.strings,
            ),
        )?;
        // Existing duplicate-principal HashSet borrows row strings. Charge its
        // real bucket envelope, including hash control groups/alignment, while
        // the DTO Vec and reader scratch are still alive. This is a concrete
        // quota table bound, not a general allocator model.
        let buckets = self
            .objects
            .checked_add(1)
            .and_then(|n| n.checked_mul(8))
            .and_then(|n| n.checked_add(6))
            .map(|n| n / 7)
            .ok_or_else(overflow)?;
        let buckets = grown_capacity(buckets)?;
        let bytes = buckets
            .checked_mul(std::mem::size_of::<&String>() + 1)
            .and_then(|n| n.checked_add(2 * 16 + std::mem::align_of::<&String>()))
            .ok_or_else(overflow)?;
        add(layout, PacketMetadataLayout::new(1, bytes))
    }
}
pub(super) fn decode(
    reader: &mut (impl Read + Seek),
    memory: &QuotaMemoryBudget,
) -> io::Result<Decoded> {
    let facts = Facts::measure(reader)?;
    // This reserve occurs before creating serde's Deserializer/IoRead scratch,
    // its first String, balance Vec, or duplicate HashSet allocation.
    let credit = memory.reserve(facts.envelope()?)?;
    let mut buffered = std::io::BufReader::with_capacity(8192, Guarded::new(reader, facts));
    let result = decode_persisted_book(&mut buffered);
    drop(buffered);
    match result {
        Ok(value) => Ok(Decoded { value, credit }),
        Err(original) => Err(io::Error::new(
            original.kind(),
            FailedDecode {
                original,
                _credit: credit,
            },
        )),
    }
}
