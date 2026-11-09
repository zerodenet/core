//! Exact original grant arrays. Clone shares the actual paid array/headers;
//! neither a bare Vec nor its capacity can escape its final credit owner.
use super::*;
#[derive(Clone, Default)]
pub struct PacketReadBatch(Option<PacketMetadataArc<Values>>);
struct Values {
    values: Box<[NativeReadGrant]>,
    _array: PacketMetadataCredit,
}
impl std::ops::Deref for PacketReadBatch {
    type Target = [NativeReadGrant];
    fn deref(&self) -> &[NativeReadGrant] {
        match &self.0 {
            Some(values) => &values.values,
            None => &[],
        }
    }
}
impl PacketReadBatch {
    fn layout(count: usize) -> Result<PacketMetadataLayout, PacketMemoryError> {
        if count == 0 {
            return Ok(PacketMetadataLayout::new(0, 0));
        }
        PacketMetadataArc::<Values>::layout()?
            .checked_add(PacketMetadataLayout::array::<NativeReadGrant>(count)?)
    }
    fn build(
        root: &Arc<PacketReturnTotals>,
        lane: Lane,
        lengths: &[usize],
        metadata: &mut PacketMetadataCredit,
    ) -> Self {
        if lengths.is_empty() {
            return Self::default();
        }
        let array = metadata
            .split(
                PacketMetadataLayout::array::<NativeReadGrant>(lengths.len())
                    .expect("actual grant array"),
            )
            .expect("same exact original array prepaid");
        let owner = metadata
            .split(PacketMetadataArc::<Values>::layout().expect("actual array/header custody"))
            .expect("same original array/header prepaid");
        let mut values = Box::<[NativeReadGrant]>::new_uninit_slice(lengths.len());
        for (target, bytes) in values.iter_mut().zip(lengths) {
            target.write(root.prepaid_io_grant(lane, *bytes, metadata));
        }
        // Every element comes from the immutable equal-length slice above.
        let values = unsafe { values.assume_init() };
        Self(Some(
            PacketMetadataArc::from_prepaid(
                Values {
                    values,
                    _array: array,
                },
                owner,
            )
            .unwrap_or_else(|_| unreachable!("same actual source array layout")),
        ))
    }
}
#[derive(Clone)]
pub struct PacketIoOwnedWorkspaces {
    pub reads: PacketReadBatch,
    pub transmit: PacketReadBatch,
}
impl PacketIoOwnedWorkspaces {
    pub fn iter(&self) -> impl Iterator<Item = &NativeReadGrant> {
        self.reads.iter().chain(self.transmit.iter())
    }
}
impl PacketReturnTotals {
    /// Same atomic pre-bind transaction; returned arrays are concrete owners,
    /// not tickets attached to elements that may outlive an empty bare Vec.
    pub fn owned_io_workspaces(
        self: &Arc<Self>,
        reads: &[usize],
        transmit: &[usize],
    ) -> Result<PacketIoOwnedWorkspaces, PacketMemoryError> {
        if reads.contains(&0) || transmit.contains(&0) {
            return Err(PacketMemoryError::Capacity);
        }
        let dimensions = |lengths: &[usize]| -> Result<PacketMemoryLimit, PacketMemoryError> {
            Ok(PacketMemoryLimit {
                frames: lengths.len(),
                bytes: lengths
                    .iter()
                    .try_fold(0usize, |total, value| total.checked_add(*value))
                    .ok_or(PacketMemoryError::Overflow)?,
            })
        };
        let count = reads
            .len()
            .checked_add(transmit.len())
            .ok_or(PacketMemoryError::Overflow)?;
        let layout = read::header::slot_layout()?
            .repeated(count)?
            .checked_add(PacketTransmitEnvelope::layout(1)?.repeated(transmit.len())?)?
            .checked_add(PacketReadBatch::layout(reads.len())?)?
            .checked_add(PacketReadBatch::layout(transmit.len())?)?;
        let mut metadata = self.prepay_io_set(dimensions(reads)?, dimensions(transmit)?, layout)?;
        let reads = PacketReadBatch::build(self, Lane::Native, reads, &mut metadata);
        let transmit = PacketReadBatch::build(self, Lane::Transmit, transmit, &mut metadata);
        Ok(PacketIoOwnedWorkspaces { reads, transmit })
    }
}

#[cfg(test)]
mod tests;
