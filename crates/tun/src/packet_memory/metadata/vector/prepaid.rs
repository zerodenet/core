//! The same paid array can be admitted before entering the concrete table lock.
use super::*;
impl<T> PacketMetadataVec<T> {
    pub fn growth_layout(
        &self,
        max_rows: usize,
    ) -> Result<Option<PacketMetadataLayout>, PacketMemoryError> {
        if self.values.len() >= max_rows {
            return Err(PacketMemoryError::Capacity);
        }
        if self.values.len() < self.values.capacity() {
            return Ok(None);
        }
        let needed = self
            .values
            .len()
            .checked_add(1)
            .ok_or(PacketMemoryError::Overflow)?;
        let capacity = self
            .values
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .min(max_rows);
        PacketMetadataLayout::array::<T>(capacity).map(Some)
    }
    /// The caller moves in a reservation from its original Root. Refunds and
    /// discarded credit are returned to the caller for destruction after locks.
    pub fn grow_prepaid(
        &mut self,
        credit: PacketMetadataCredit,
        max_rows: usize,
    ) -> Result<Option<PacketMetadataCredit>, (PacketMemoryError, PacketMetadataCredit)> {
        let layout = match self.growth_layout(max_rows) {
            Ok(Some(layout)) => layout,
            Ok(None) => return Ok(Some(credit)),
            Err(source) => return Err((source, credit)),
        };
        if credit.layout() != layout
            || self
                .credit
                .as_ref()
                .is_some_and(|old| !old.same_counter(&credit))
        {
            return Err((PacketMemoryError::Capacity, credit));
        }
        let capacity = if std::mem::size_of::<T>() == 0 {
            usize::MAX
        } else {
            layout.bytes() / std::mem::size_of::<T>()
        };
        let slots = Box::<[T]>::new_uninit_slice(capacity);
        let pointer = Box::into_raw(slots) as *mut T;
        // SAFETY: exact prepaid array<T>, with no initialized element yet.
        let next = unsafe { Vec::from_raw_parts(pointer, 0, capacity) };
        let mut previous = std::mem::replace(&mut self.values, next);
        self.values.append(&mut previous);
        drop(previous); // actual backing free before returning its old promise
        Ok(self.credit.replace(credit))
    }
}
