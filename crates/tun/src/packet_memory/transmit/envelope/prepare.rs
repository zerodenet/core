use super::*;
impl PacketTransmitEnvelope {
    /// Called only at a concrete producer's pre-FIFO boundary. Received data
    /// may become an actual forward only after authentication/routing; extend
    /// that same original's finite reservation there, never in a wire driver.
    pub(in crate::packet_memory::transmit) fn prepare(
        &self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), PacketMemoryError> {
        let root = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remaining
            .root
            .clone();
        let _notifications = root.defer_notifications();
        let wake = std::task::Waker::from(root.clone());
        let split_wake = std::task::Waker::from(root.clone());
        let array_changed = std::task::Waker::from(root.clone());
        let array_refunded = std::task::Waker::from(root.clone());
        let mut reserved = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let add_frames = frames.saturating_sub(reserved.remaining.frames);
        let add_bytes = bytes.saturating_sub(reserved.remaining.bytes);
        let total_frames = reserved
            .total
            .frames
            .checked_add(add_frames)
            .and_then(|v| v.checked_add(reserved.anchor.frames))
            .ok_or(PacketMemoryError::Overflow)?;
        let total_bytes = reserved
            .total
            .bytes
            .checked_add(add_bytes)
            .and_then(|v| v.checked_add(reserved.anchor.bytes))
            .ok_or(PacketMemoryError::Overflow)?;
        let mut state = root.state();
        if total_frames > state.transmit.limit.frames || total_bytes > state.transmit.limit.bytes {
            return Err(PacketMemoryError::Capacity);
        }
        if state.paused || state.metadata_pauses != 0 {
            return Err(PacketMemoryError::Paused);
        }
        if add_frames
            > state
                .transmit
                .limit
                .frames
                .saturating_sub(state.transmit.occupied.frames)
            || add_bytes
                > state
                    .transmit
                    .limit
                    .bytes
                    .saturating_sub(state.transmit.occupied.bytes)
        {
            return Err(PacketMemoryError::Busy);
        }
        let target = reserved
            .total
            .frames
            .checked_add(add_frames)
            .ok_or(PacketMemoryError::Overflow)?;
        let count =
            target.saturating_sub(reserved.headers.as_ref().expect("same headers").capacity());
        let array_count = if target > 1 {
            target.saturating_sub(
                reserved
                    .arrays
                    .as_ref()
                    .map_or(0, |arrays| arrays.capacity()),
            )
        } else {
            0
        };
        let header_layout = NativePacketCredit::header_layout().repeated(count)?;
        let array_layout = if array_count == 0 {
            PacketMetadataLayout::new(0, 0)
        } else if reserved.arrays.is_some() {
            Self::array_unit().repeated(array_count)?
        } else {
            PacketMetadataHeaderPromise::layout(Self::array_unit(), array_count)?
        };
        let layout = header_layout.checked_add(array_layout)?;
        let mut metadata = if count != 0 || array_count != 0 {
            Some(root.metadata.reserve(layout, wake)?)
        } else {
            None
        };
        let emptied_header = if count != 0 {
            let credit = metadata
                .as_mut()
                .expect("same growth")
                .split_prepared(header_layout, split_wake)
                .unwrap_or_else(|_| unreachable!("same concrete header growth"));
            Some(
                reserved
                    .headers
                    .as_ref()
                    .expect("same headers")
                    .grow(count, credit)
                    .unwrap_or_else(|_| unreachable!("same original header counter")),
            )
        } else {
            None
        };
        let emptied_array = if array_count != 0 {
            let credit = metadata.take().expect("same original array prepayment");
            if let Some(arrays) = &reserved.arrays {
                Some(
                    arrays
                        .grow(array_count, credit)
                        .unwrap_or_else(|_| unreachable!("same actual array counter")),
                )
            } else {
                reserved.arrays = Some(
                    PacketMetadataHeaderPromise::from_prepaid(
                        Self::array_unit(),
                        array_count,
                        array_count,
                        credit,
                        array_changed,
                        array_refunded,
                    )
                    .unwrap_or_else(|_| unreachable!("same concrete array promise was prepaid")),
                );
                None
            }
        } else {
            None
        };
        state.transmit.occupied.frames += add_frames;
        state.transmit.occupied.bytes += add_bytes;
        reserved.remaining.frames += add_frames;
        reserved.remaining.bytes += add_bytes;
        reserved.total.frames += add_frames;
        reserved.total.bytes += add_bytes;
        drop(state);
        drop(reserved);
        drop(emptied_header);
        drop(emptied_array);
        drop(metadata); // same original Wakers/Arcs, always outside all owner locks
        Ok(())
    }
}
