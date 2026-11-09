//! One existing source cache using prepaid owned storage. Fragments are copied
//! into that one charged assembly, never retained as unaccounted Vec images.
mod allocate;
mod header;
mod packet;
mod ranges;
mod rows;
use super::*;
use header::Header;
use packet::Reassembled;
use rows::Rows;
use std::hash::Hash;
use zero_traits::{PacketBuffer, PacketStorage};

pub struct OwnedFragmentReassembler<K> {
    assemblies: Rows<((K, FragmentKey), Assembly)>,
    buffered: usize,
    last_cleanup: Instant,
    #[cfg(feature = "packet-memory")]
    memory: Option<zero_tun::packet_memory::PacketMemoryView>,
}
struct Assembly {
    storage: PacketBuffer,
    header: Header,
    ranges: Rows<(usize, usize)>,
    #[cfg(feature = "packet-memory")]
    header_credit: Option<zero_tun::packet_memory::PacketMetadataCredit>,
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
            assemblies: Rows::new(),
            buffered: 0,
            last_cleanup: Instant::now(),
            #[cfg(feature = "packet-memory")]
            memory: None,
        }
    }
    #[cfg(feature = "packet-memory")]
    pub fn new_with_memory(
        memory: zero_tun::packet_memory::PacketMemoryView,
    ) -> Result<Self, zero_tun::packet_memory::PacketMemoryError> {
        let metadata = memory.driver_metadata_view()?;
        Ok(Self {
            assemblies: Rows::with_memory(metadata),
            buffered: 0,
            last_cleanup: Instant::now(),
            memory: Some(memory),
        })
    }
    /// Discard exactly this source generation's fragment state, retaining the
    /// same actual Root binding. It does not create another library cache.
    pub fn reset(&mut self) {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.assemblies.notifications();
        let replacement = self.assemblies.sibling();
        let previous = std::mem::replace(&mut self.assemblies, replacement);
        self.buffered = 0;
        self.last_cleanup = Instant::now();
        drop(previous);
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
        if !self
            .assemblies
            .iter()
            .any(|(candidate, _)| candidate == &key)
        {
            if self.assemblies.len() >= MAX_FRAGMENT_ASSEMBLIES {
                return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
            }
            // Pay actual row/range/header layouts before the new assembly's
            // independent Native payload allocation. No old assembly is evicted.
            {
                #[cfg(feature = "packet-memory")]
                let _notifications = self.assemblies.notifications();
                if self
                    .assemblies
                    .reserve_one(MAX_FRAGMENT_ASSEMBLIES)
                    .is_err()
                {
                    return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
                }
            }
            let mut ranges: Rows<(usize, usize)> = self.assemblies.sibling();
            {
                #[cfg(feature = "packet-memory")]
                let _notifications = ranges.notifications();
                if ranges
                    .reserve_one(header::MAX_STORAGE.div_ceil(8) + 1)
                    .is_err()
                {
                    return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
                }
            }
            #[cfg(feature = "packet-memory")]
            let header_credit = match self.assemblies.header() {
                Ok(credit) => credit,
                Err(reason) => return OwnedFragmentOutcome::Rejected(reason),
            };
            // An existing IO workspace remains prepaid separately, so denying
            // this new assembly cannot consume the next read needed by old ones.
            let storage = match self.allocate(&packet).await {
                Ok(storage) => storage,
                Err(reason) => return OwnedFragmentOutcome::Rejected(reason),
            };
            if storage.len() < header::MAX_STORAGE {
                return OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
            }
            self.assemblies.push_reserved((
                key.clone(),
                Assembly {
                    storage,
                    header,
                    ranges,
                    #[cfg(feature = "packet-memory")]
                    header_credit,
                    first: false,
                    final_length: None,
                    buffered: 0,
                    updated: now,
                },
            ));
        }
        match self.insert(&key, &packet, &fragment, header, now) {
            Ok(false) => OwnedFragmentOutcome::Pending,
            Err(reason) => {
                if reason != FragmentRejectReason::ResourceLimit {
                    self.remove(&key);
                }
                OwnedFragmentOutcome::Rejected(reason)
            }
            Ok(true) => {
                let mut assembly = self.take(&key).expect("same completed assembly");
                self.buffered -= assembly.buffered;
                let length = assembly.final_length.expect("complete final length");
                match assembly.header.finish(&mut assembly.storage, length) {
                    Some(length) => OwnedFragmentOutcome::Packet {
                        packet: assembly.finish_packet(length),
                        reassembled: true,
                    },
                    None => OwnedFragmentOutcome::Rejected(FragmentRejectReason::InvalidLength),
                }
            }
        }
    }
    pub fn cleanup_expired(&mut self, now: Instant) -> usize {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.assemblies.notifications();
        let before = self.assemblies.len();
        let mut index = 0;
        while index < self.assemblies.len() {
            if now.saturating_duration_since(self.assemblies[index].1.updated) >= FRAGMENT_EXPIRY {
                let (_, entry) = self.assemblies.remove(index);
                self.buffered -= entry.buffered;
                drop(entry);
            } else {
                index += 1;
            }
        }
        self.assemblies.release_empty();
        self.last_cleanup = now;
        before - self.assemblies.len()
    }
    pub fn buffered_bytes(&self) -> usize {
        self.buffered
    }
    pub fn pending_datagrams(&self) -> usize {
        self.assemblies.len()
    }
    fn take(&mut self, key: &(K, FragmentKey)) -> Option<Assembly> {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.assemblies.notifications();
        let index = self
            .assemblies
            .iter()
            .position(|(candidate, _)| candidate == key)?;
        let entry = self.assemblies.remove(index).1;
        self.assemblies.release_empty();
        Some(entry)
    }
    fn remove(&mut self, key: &(K, FragmentKey)) {
        #[cfg(feature = "packet-memory")]
        let _notifications = self.assemblies.notifications();
        if let Some(entry) = self.take(key) {
            self.buffered -= entry.buffered;
            drop(entry);
        }
    }
}
impl Assembly {
    fn finish_packet(self, length: usize) -> PacketBuffer {
        let storage = Reassembled {
            storage: self.storage,
            length,
        };
        #[cfg(feature = "packet-memory")]
        if let Some(header) = self.header_credit {
            return PacketBuffer::from_prepaid_owner(storage, header)
                .unwrap_or_else(|_| unreachable!("same original assembly header was prepaid"));
        }
        PacketBuffer::from_owner(storage)
    }
}

#[cfg(all(test, feature = "packet-memory"))]
mod tests;
