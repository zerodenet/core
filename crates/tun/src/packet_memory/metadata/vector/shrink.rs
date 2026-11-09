//! Exact same table maintenance, old+new requested arrays both prepaid.
use super::*;
impl<T> PacketMetadataVec<T> {
    pub fn try_shrink_to_fit_with_metadata(
        &mut self,
        memory: &PacketDriverMetadataView,
    ) -> Result<(), PacketMemoryError> {
        let capacity = self.values.len();
        if capacity == self.values.capacity() {
            return Ok(());
        }
        if capacity == 0 {
            drop(std::mem::take(&mut self.values));
            drop(self.credit.take());
            return Ok(());
        }
        let layout = PacketMetadataLayout::array::<T>(capacity)?;
        let next_credit = memory.reserve(layout)?;
        if self
            .credit
            .as_ref()
            .is_some_and(|old| !old.same_counter(&next_credit))
        {
            return Err(PacketMemoryError::Capacity);
        }
        let slots = Box::<[T]>::new_uninit_slice(capacity);
        let pointer = Box::into_raw(slots) as *mut T;
        // SAFETY: same exact array<T> layout paid above, initially no initialized T.
        let next = unsafe { Vec::from_raw_parts(pointer, 0, capacity) };
        let mut previous = std::mem::replace(&mut self.values, next);
        self.values.extend(previous.drain(..));
        drop(previous);
        drop(self.credit.replace(next_credit));
        Ok(())
    }
}
