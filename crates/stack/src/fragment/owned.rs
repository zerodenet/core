//! One existing source cache using prepaid owned storage. Fragments are copied
//! into that one charged assembly, never retained as unaccounted Vec images.
mod header;
use super::*;
use header::Header;
use std::hash::Hash;
use zero_traits::{PacketBuffer, PacketStorage};

pub struct OwnedFragmentReassembler<K> {
    assemblies: HashMap<(K, FragmentKey), Assembly>,
    buffered: usize,
    last_cleanup: Instant,
}
struct Assembly {
    storage: PacketBuffer,
    header: Header,
    ranges: BTreeMap<usize, usize>,
    first: bool,
    final_length: Option<usize>,
    buffered: usize,
    updated: Instant,
}
impl<K: Clone + Eq + Hash> Default for OwnedFragmentReassembler<K> {
    fn default() -> Self {
        Self::new()
    }
}
impl<K: Clone + Eq + Hash> OwnedFragmentReassembler<K> {
    pub fn new() -> Self {
        Self {
            assemblies: HashMap::new(),
            buffered: 0,
            last_cleanup: Instant::now(),
        }
    }
    /// The namespace is the caller's exact authenticated source/session cookie.
    /// This algorithm treats it as opaque and mints no routing or IO identity.
    pub async fn process(
        &mut self,
        namespace: K,
        packet: PacketBuffer,
        now: Instant,
    ) -> OwnedFragmentOutcome<PacketBuffer> {
        if now.saturating_duration_since(self.last_cleanup) >= CLEANUP_INTERVAL {
            self.cleanup_expired(now);
        }
        let Some(fragment) = parse_ip_fragment(&packet) else {
            return OwnedFragmentOutcome::Packet {
                packet,
                reassembled: false,
            };
        };
        let key = (namespace, fragment.key.clone());
        if let Some(reason) = validate_fragment(&fragment) {
            self.remove(&key);
            return OwnedFragmentOutcome::Rejected(reason);
        }
        let Some(header) = Header::parse(&packet, &fragment) else {
            self.remove(&key);
            return OwnedFragmentOutcome::Rejected(FragmentRejectReason::InvalidLength);
        };
        if !self.assemblies.contains_key(&key) {
            if self.assemblies.len() >= MAX_FRAGMENT_ASSEMBLIES {
                return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
            }
            // An existing IO workspace remains prepaid separately, so denying
            // this new assembly cannot consume the next read needed by old ones.
            let mut storage = match packet.derive_zeroed(&[header::MAX_STORAGE]).await {
                Ok(storage) if storage.len() == 1 => storage,
                _ => return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit),
            };
            let storage = storage.pop().expect("one actual assembly allocation");
            if storage.len() < header::MAX_STORAGE {
                return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
            }
            self.assemblies.insert(
                key.clone(),
                Assembly {
                    storage,
                    header,
                    ranges: BTreeMap::new(),
                    first: false,
                    final_length: None,
                    buffered: 0,
                    updated: now,
                },
            );
        }
        match self.insert(&key, &packet, &fragment, header, now) {
            Ok(false) => OwnedFragmentOutcome::Pending,
            Err(reason) => {
                self.remove(&key);
                OwnedFragmentOutcome::Rejected(reason)
            }
            Ok(true) => {
                let mut assembly = self
                    .assemblies
                    .remove(&key)
                    .expect("same completed assembly");
                self.buffered -= assembly.buffered;
                let length = assembly.final_length.expect("complete final length");
                match assembly.header.finish(&mut assembly.storage, length) {
                    Some(length) => OwnedFragmentOutcome::Packet {
                        packet: PacketBuffer::from_owner(Reassembled {
                            storage: assembly.storage,
                            length,
                        }),
                        reassembled: true,
                    },
                    None => OwnedFragmentOutcome::Rejected(FragmentRejectReason::InvalidLength),
                }
            }
        }
    }
    fn insert(
        &mut self,
        key: &(K, FragmentKey),
        packet: &[u8],
        fragment: &ParsedIpFragment<'_>,
        header: Header,
        now: Instant,
    ) -> Result<bool, FragmentRejectReason> {
        let entry = self
            .assemblies
            .get_mut(key)
            .expect("same admitted assembly");
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
        if let Some(existing) = entry.ranges.get(&offset) {
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
            .any(|(&start, &len)| ranges_overlap(offset, end, start, start + len))
        {
            return Err(FragmentRejectReason::Overlap);
        }
        if !fragment.more_fragments {
            if entry.ranges.iter().any(|(&start, &len)| start + len > end)
                || entry.final_length.is_some_and(|current| current != end)
            {
                return Err(FragmentRejectReason::ConflictingFinalLength);
            }
            entry.final_length = Some(end);
        }
        if entry.final_length.is_some_and(|last| end > last) {
            return Err(FragmentRejectReason::ConflictingFinalLength);
        }
        if self.buffered.saturating_add(length) > MAX_FRAGMENT_BUFFER_BYTES {
            return Err(FragmentRejectReason::ResourceLimit);
        }
        if offset == 0 {
            entry.header.copy_first(&mut entry.storage, packet);
            entry.first = true;
        }
        entry.storage[placed..placed_end].copy_from_slice(fragment.payload);
        entry.ranges.insert(offset, length);
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
        for (&offset, &length) in &entry.ranges {
            if offset != cursor {
                return Ok(false);
            }
            cursor += length;
        }
        Ok(cursor == final_length)
    }
    pub fn cleanup_expired(&mut self, now: Instant) -> usize {
        let before = self.assemblies.len();
        self.assemblies.retain(|_, entry| {
            if now.saturating_duration_since(entry.updated) >= FRAGMENT_EXPIRY {
                self.buffered -= entry.buffered;
                false
            } else {
                true
            }
        });
        self.last_cleanup = now;
        before - self.assemblies.len()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.buffered
    }
    pub fn pending_datagrams(&self) -> usize {
        self.assemblies.len()
    }
    fn remove(&mut self, key: &(K, FragmentKey)) {
        if let Some(entry) = self.assemblies.remove(key) {
            self.buffered -= entry.buffered;
        }
    }
}
struct Reassembled {
    storage: PacketBuffer,
    length: usize,
}
impl AsRef<[u8]> for Reassembled {
    fn as_ref(&self) -> &[u8] {
        &self.storage[..self.length]
    }
}
impl AsMut<[u8]> for Reassembled {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.storage[..self.length]
    }
}
impl PacketStorage for Reassembled {
    fn prepare_transmit(
        &mut self,
        frames: usize,
        bytes: usize,
    ) -> Result<(), zero_traits::PacketDeriveError> {
        self.storage.prepare_transmit(frames, bytes)
    }
    fn try_derive_zeroed(
        &self,
        lengths: &[usize],
    ) -> Result<Vec<PacketBuffer>, zero_traits::PacketDeriveError> {
        self.storage.try_derive_zeroed(lengths)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        let mut bytes = self.storage.take_vec_allocation()?;
        bytes.truncate(self.length);
        Some(bytes)
    }
    fn allocation_bytes(&self) -> usize {
        self.storage.allocation_bytes()
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self.storage.derive_zeroed(lengths)
    }
}
