//! Same Source table allocation, not a second census or resource scheduler.
//! Moving/removing a row does not refund its still-allocated table capacity.
use super::*;
use std::ops::Deref;

pub struct PacketMetadataVec<T> {
    values: Vec<T>,
    credit: Option<PacketMetadataCredit>,
}
impl<T> Default for PacketMetadataVec<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            credit: None,
        }
    }
}
impl<T> PacketMetadataVec<T> {
    /// The actual caller supplies its existing finite row bound. Its owning
    /// Source must defer Root notifications BEFORE taking its table mutex;
    /// this synchronous method never creates a task or waits for its own rows.
    pub fn try_reserve_one(
        &mut self,
        memory: &PacketMemoryView,
        max_rows: usize,
    ) -> Result<(), PacketMemoryError> {
        if self.values.len() >= max_rows {
            return Err(PacketMemoryError::Capacity);
        }
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        let Some(needed) = self.values.len().checked_add(1) else {
            return Err(PacketMemoryError::Overflow);
        };
        let new_capacity = self
            .values
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .min(max_rows);
        let layout = match PacketMetadataLayout::array::<T>(new_capacity) {
            Ok(layout) => layout,
            Err(source) => return Err(source),
        };
        // The old live table cannot be evicted to satisfy this growth. If its
        // own old+new layouts can never fit this installed Root, return a real
        // permanent capacity error instead of an impossible Pending retry.
        let old = self
            .credit
            .as_ref()
            .map(PacketMetadataCredit::layout)
            .unwrap_or(PacketMetadataLayout::new(0, 0));
        let peak = match old.checked_add(layout).and_then(|peak| {
            peak.checked_add(zero_traits::packet_metadata::PacketMetadataCounter::owned_layout())
        }) {
            Ok(layout) => layout,
            Err(source) => return Err(source),
        };
        let limit = memory.driver_metadata().limit;
        if peak.allocations() > limit.allocations() || peak.bytes() > limit.bytes() {
            return Err(PacketMemoryError::Capacity);
        }
        let next_credit = match memory.reserve_driver_metadata(layout) {
            Ok(credit) => credit,
            Err(source) => return Err(source),
        };
        // Box's requested array Layout is EXACTLY the one prepaid above. No
        // opaque reserve/realloc operation can create a larger reported Vec
        // capacity or hide simultaneous old/new storage from the Root.
        let slots = Box::<[T]>::new_uninit_slice(new_capacity);
        let pointer = Box::into_raw(slots) as *mut T;
        // SAFETY: MaybeUninit<T> has T's size/alignment, the Global allocation
        // is precisely array<T>(new_capacity), and initially NO T is live.
        // Vec owns that same allocation and drops only subsequently moved T.
        let next = unsafe { Vec::from_raw_parts(pointer, 0, new_capacity) };
        let mut previous = std::mem::replace(&mut self.values, next);
        self.values.extend(previous.drain(..)); // sufficient existing capacity
        drop(previous); // actual old allocation is free before refund
        let previous_credit = self.credit.replace(next_credit);
        drop(previous_credit);
        Ok(())
    }
    pub fn try_push(
        &mut self,
        value: T,
        memory: &PacketMemoryView,
        max_rows: usize,
    ) -> Result<(), (PacketMemoryError, T)> {
        if let Err(source) = self.try_reserve_one(memory, max_rows) {
            return Err((source, value));
        }
        self.push_reserved(value)
            .map_err(|value| (PacketMemoryError::Capacity, value))
    }
    /// Consume only capacity already charged on THIS actual table. No reserve,
    /// reallocation or callback occurs while installing the real record.
    pub fn push_reserved(&mut self, value: T) -> Result<(), T> {
        if self.values.len() == self.values.capacity() {
            return Err(value);
        }
        self.values.push(value);
        Ok(())
    }
    pub fn remove(&mut self, index: usize) -> T {
        self.values.remove(index)
    }
    pub fn capacity(&self) -> usize {
        self.values.capacity()
    }
}
impl<T> Deref for PacketMetadataVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}
impl<T> Drop for PacketMetadataVec<T> {
    fn drop(&mut self) {
        // Even when a payload destructor unwinds, Vec's own deallocation guard
        // runs before this struct's remaining credit field is dropped.
        drop(std::mem::take(&mut self.values));
        drop(self.credit.take());
    }
}

#[cfg(test)]
mod tests;
