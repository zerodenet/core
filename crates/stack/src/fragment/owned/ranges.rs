use super::*;
impl<K: Clone + Eq + Hash> OwnedFragmentReassembler<K> {
    pub(super) fn insert(
        &mut self,
        key: &(K, FragmentKey),
        packet: &[u8],
        fragment: &ParsedIpFragment<'_>,
        header: Header,
        now: Instant,
    ) -> Result<bool, FragmentRejectReason> {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.assemblies.notifications();
        let entry = &mut self
            .assemblies
            .iter_mut()
            .find(|(candidate, _)| candidate == key)
            .expect("same admitted assembly")
            .1;
        if entry.header != header {
            return Err(FragmentRejectReason::InvalidLength);
        }
        let offset = fragment.offset;
        let length = fragment.payload.len();
        let end = offset + length;
        let placed = entry.header.length + offset;
        let placed_end = placed
            .checked_add(length)
            .ok_or(FragmentRejectReason::InvalidLength)?;
        if placed_end > entry.storage.len() {
            return Err(FragmentRejectReason::InvalidLength);
        }
        if let Some(existing) = entry
            .ranges
            .iter()
            .find(|(start, _)| *start == offset)
            .map(|(_, length)| length)
        {
            if *existing == length
                && entry.storage[placed..placed_end] == *fragment.payload
                && (offset != 0 || entry.header.matches_first(&entry.storage, packet))
            {
                entry.updated = now;
                return Ok(false);
            }
            return Err(FragmentRejectReason::Overlap);
        }
        if entry
            .ranges
            .iter()
            .any(|&(start, len)| ranges_overlap(offset, end, start, start + len))
        {
            return Err(FragmentRejectReason::Overlap);
        }
        if !fragment.more_fragments {
            if entry.ranges.iter().any(|&(start, len)| start + len > end)
                || entry.final_length.is_some_and(|current| current != end)
            {
                return Err(FragmentRejectReason::ConflictingFinalLength);
            }
        }
        if entry.final_length.is_some_and(|last| end > last) {
            return Err(FragmentRejectReason::ConflictingFinalLength);
        }
        if self.buffered.saturating_add(length) > MAX_FRAGMENT_BUFFER_BYTES {
            return Err(FragmentRejectReason::ResourceLimit);
        }
        entry
            .ranges
            .reserve_one(header::MAX_STORAGE.div_ceil(8) + 1)?;
        if !fragment.more_fragments {
            entry.final_length = Some(end);
        }
        if offset == 0 {
            entry.header.copy_first(&mut entry.storage, packet);
            entry.first = true;
        }
        entry.storage[placed..placed_end].copy_from_slice(fragment.payload);
        let position = entry
            .ranges
            .iter()
            .position(|(start, _)| *start > offset)
            .unwrap_or(entry.ranges.len());
        entry.ranges.push_reserved((offset, length));
        entry.ranges[position..].rotate_right(1);
        entry.buffered += length;
        self.buffered += length;
        entry.updated = now;
        let Some(final_length) = entry.final_length else {
            return Ok(false);
        };
        if !entry.first {
            return Ok(false);
        }
        let mut cursor = 0;
        for &(offset, length) in entry.ranges.iter() {
            if offset != cursor {
                return Ok(false);
            }
            cursor += length;
        }
        Ok(cursor == final_length)
    }
}
