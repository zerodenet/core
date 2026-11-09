//! One inline packet or a concrete prepaid array of packets. The array's
//! lease follows its actual Vec/IntoIter deallocation, never an element Drop.
use crate::packet_metadata::{PacketMemoryError, PacketMetadataHeaderLease, PacketMetadataLayout};
use crate::PacketBuffer;
use alloc::vec::{IntoIter, Vec};
use core::ops::{Deref, DerefMut};
#[derive(Default)]
pub struct PacketBatch {
    values: Values,
}
#[derive(Default)]
enum Values {
    #[default]
    Empty,
    One(Option<PacketBuffer>),
    Array {
        values: Vec<PacketBuffer>,
        _lease: Option<PacketMetadataHeaderLease>,
    },
}
impl PacketBatch {
    pub fn one(packet: PacketBuffer) -> Self {
        Self {
            values: Values::One(Some(packet)),
        }
    }
    pub fn from_prepaid_array(
        values: Vec<PacketBuffer>,
        lease: PacketMetadataHeaderLease,
    ) -> Result<
        Self,
        (
            PacketMemoryError,
            Vec<PacketBuffer>,
            PacketMetadataHeaderLease,
        ),
    > {
        let expected = match PacketMetadataLayout::array::<PacketBuffer>(values.capacity()) {
            Ok(layout) => layout,
            Err(source) => return Err((source, values, lease)),
        };
        let paid = lease.layout();
        if paid.allocations() < expected.allocations() || paid.bytes() < expected.bytes() {
            return Err((PacketMemoryError::Capacity, values, lease));
        }
        Ok(Self {
            values: Values::Array {
                values,
                _lease: Some(lease),
            },
        })
    }
    /// Explicit independent Vec-library compatibility. Kernel assembly must
    /// use One or from_prepaid_array; this method creates no admission evidence.
    pub fn from_library(values: Vec<PacketBuffer>) -> Self {
        Self {
            values: Values::Array {
                values,
                _lease: None,
            },
        }
    }
    /// Replace elements in the SAME already paid array. The temporary empty
    /// packet has no heap allocation; even unwinding keeps the original lease.
    pub fn map_in_place(mut self, mut map: impl FnMut(PacketBuffer) -> PacketBuffer) -> Self {
        for packet in self.iter_mut() {
            *packet = map(core::mem::replace(packet, PacketBuffer::from(Vec::new())));
        }
        self
    }
    pub fn pop(&mut self) -> Option<PacketBuffer> {
        match &mut self.values {
            Values::Empty => None,
            Values::One(packet) => packet.take(),
            Values::Array { values, .. } => values.pop(),
        }
    }
    pub fn remove(&mut self, index: usize) -> PacketBuffer {
        match &mut self.values {
            Values::Array { values, .. } => values.remove(index),
            Values::One(packet) if index == 0 => packet.take().expect("nonempty original batch"),
            _ => panic!("original packet batch index out of bounds"),
        }
    }
}
impl Deref for PacketBatch {
    type Target = [PacketBuffer];
    fn deref(&self) -> &[PacketBuffer] {
        match &self.values {
            Values::Empty => &[],
            Values::One(packet) => packet.as_slice(),
            Values::Array { values, .. } => values,
        }
    }
}
impl DerefMut for PacketBatch {
    fn deref_mut(&mut self) -> &mut [PacketBuffer] {
        match &mut self.values {
            Values::Empty => &mut [],
            Values::One(packet) => packet.as_mut_slice(),
            Values::Array { values, .. } => values,
        }
    }
}
pub struct PacketBatchIntoIter {
    values: IterValues,
}
enum IterValues {
    Empty,
    One(Option<PacketBuffer>),
    Array {
        values: IntoIter<PacketBuffer>,
        _lease: Option<PacketMetadataHeaderLease>,
    },
}
impl IntoIterator for PacketBatch {
    type Item = PacketBuffer;
    type IntoIter = PacketBatchIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        PacketBatchIntoIter {
            values: match self.values {
                Values::Empty => IterValues::Empty,
                Values::One(value) => IterValues::One(value),
                Values::Array { values, _lease } => IterValues::Array {
                    values: values.into_iter(),
                    _lease,
                },
            },
        }
    }
}
impl Iterator for PacketBatchIntoIter {
    type Item = PacketBuffer;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.values {
            IterValues::Empty => None,
            IterValues::One(value) => value.take(),
            IterValues::Array { values, .. } => values.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.values {
            IterValues::Empty => (0, Some(0)),
            IterValues::One(value) => {
                let length = usize::from(value.is_some());
                (length, Some(length))
            }
            IterValues::Array { values, .. } => values.size_hint(),
        }
    }
}
impl ExactSizeIterator for PacketBatchIntoIter {}
impl DoubleEndedIterator for PacketBatchIntoIter {
    fn next_back(&mut self) -> Option<PacketBuffer> {
        match &mut self.values {
            IterValues::Empty => None,
            IterValues::One(value) => value.take(),
            IterValues::Array { values, .. } => values.next_back(),
        }
    }
}

impl<'a> IntoIterator for &'a PacketBatch {
    type Item = &'a PacketBuffer;
    type IntoIter = core::slice::Iter<'a, PacketBuffer>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<'a> IntoIterator for &'a mut PacketBatch {
    type Item = &'a mut PacketBuffer;
    type IntoIter = core::slice::IterMut<'a, PacketBuffer>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
impl core::fmt::Debug for PacketBatch {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
