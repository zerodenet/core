use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use crate::packet::{parse_ip_fragment, rebuild_fragmented_packet, FragmentKey, ParsedIpFragment};

const MAX_FRAGMENT_ASSEMBLIES: usize = 256;
const MAX_FRAGMENT_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const MAX_REASSEMBLED_PAYLOAD_BYTES: usize = u16::MAX as usize;
const FRAGMENT_EXPIRY: Duration = Duration::from_secs(30);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentRejectReason {
    InvalidLength,
    Overlap,
    ConflictingFinalLength,
    ResourceLimit,
    MissingFirstFragment,
}

pub enum FragmentOutcome<'a> {
    NotFragmented(&'a [u8]),
    Pending,
    Reassembled(Vec<u8>),
    Rejected(FragmentRejectReason),
}

pub struct FragmentReassembler {
    assemblies: HashMap<FragmentKey, Assembly>,
    total_buffered: usize,
    last_cleanup: Instant,
}

struct Assembly {
    fragments: BTreeMap<usize, Vec<u8>>,
    first_fragment: Option<Vec<u8>>,
    final_length: Option<usize>,
    buffered: usize,
    updated_at: Instant,
}

impl Default for FragmentReassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl FragmentReassembler {
    pub fn new() -> Self {
        Self {
            assemblies: HashMap::new(),
            total_buffered: 0,
            last_cleanup: Instant::now(),
        }
    }

    pub fn process<'a>(&mut self, packet: &'a [u8], now: Instant) -> FragmentOutcome<'a> {
        self.cleanup_if_due(now);
        let Some(fragment) = parse_ip_fragment(packet) else {
            return FragmentOutcome::NotFragmented(packet);
        };
        if let Some(reason) = validate_fragment(&fragment) {
            self.remove(&fragment.key);
            return FragmentOutcome::Rejected(reason);
        }

        if !self.assemblies.contains_key(&fragment.key)
            && self.assemblies.len() >= MAX_FRAGMENT_ASSEMBLIES
        {
            return FragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
        }

        let end = fragment.offset + fragment.payload.len();
        let entry = self
            .assemblies
            .entry(fragment.key.clone())
            .or_insert_with(|| Assembly {
                fragments: BTreeMap::new(),
                first_fragment: None,
                final_length: None,
                buffered: 0,
                updated_at: now,
            });

        if let Some(existing) = entry.fragments.get(&fragment.offset) {
            let identical_first = fragment.offset != 0
                || entry
                    .first_fragment
                    .as_deref()
                    .is_some_and(|first| first == packet);
            if existing.as_slice() == fragment.payload && identical_first {
                entry.updated_at = now;
                return FragmentOutcome::Pending;
            }
            self.remove(&fragment.key);
            return FragmentOutcome::Rejected(FragmentRejectReason::Overlap);
        }
        if entry.fragments.iter().any(|(&offset, payload)| {
            ranges_overlap(fragment.offset, end, offset, offset + payload.len())
        }) {
            self.remove(&fragment.key);
            return FragmentOutcome::Rejected(FragmentRejectReason::Overlap);
        }
        if !fragment.more_fragments {
            if entry
                .fragments
                .iter()
                .any(|(&offset, payload)| offset.saturating_add(payload.len()) > end)
            {
                self.remove(&fragment.key);
                return FragmentOutcome::Rejected(FragmentRejectReason::ConflictingFinalLength);
            }
            if entry.final_length.is_some_and(|length| length != end) {
                self.remove(&fragment.key);
                return FragmentOutcome::Rejected(FragmentRejectReason::ConflictingFinalLength);
            }
            entry.final_length = Some(end);
        }
        if entry.final_length.is_some_and(|length| end > length) {
            self.remove(&fragment.key);
            return FragmentOutcome::Rejected(FragmentRejectReason::ConflictingFinalLength);
        }
        if self.total_buffered.saturating_add(fragment.payload.len()) > MAX_FRAGMENT_BUFFER_BYTES {
            self.remove(&fragment.key);
            return FragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit);
        }

        if fragment.offset == 0 {
            entry.first_fragment = Some(packet.to_vec());
        }
        entry.buffered += fragment.payload.len();
        entry.updated_at = now;
        entry
            .fragments
            .insert(fragment.offset, fragment.payload.to_vec());
        self.total_buffered += fragment.payload.len();

        let Some(final_length) = entry.final_length else {
            return FragmentOutcome::Pending;
        };
        let Some(first_fragment) = entry.first_fragment.as_ref() else {
            return FragmentOutcome::Pending;
        };
        let mut cursor = 0;
        for (&offset, payload) in &entry.fragments {
            if offset != cursor {
                return FragmentOutcome::Pending;
            }
            cursor += payload.len();
        }
        if cursor != final_length {
            return FragmentOutcome::Pending;
        }

        let mut payload = vec![0_u8; final_length];
        for (&offset, fragment) in &entry.fragments {
            payload[offset..offset + fragment.len()].copy_from_slice(fragment);
        }
        let first_fragment = first_fragment.clone();
        self.remove(&fragment.key);
        match rebuild_fragmented_packet(&first_fragment, &payload) {
            Some(packet) => FragmentOutcome::Reassembled(packet),
            None => FragmentOutcome::Rejected(FragmentRejectReason::MissingFirstFragment),
        }
    }

    pub fn cleanup_expired(&mut self, now: Instant) -> usize {
        let expired = self
            .assemblies
            .iter()
            .filter_map(|(key, assembly)| {
                (now.saturating_duration_since(assembly.updated_at) >= FRAGMENT_EXPIRY)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        let count = expired.len();
        for key in expired {
            self.remove(&key);
        }
        self.last_cleanup = now;
        count
    }

    pub fn buffered_bytes(&self) -> usize {
        self.total_buffered
    }

    pub fn pending_datagrams(&self) -> usize {
        self.assemblies.len()
    }

    fn cleanup_if_due(&mut self, now: Instant) {
        if now.saturating_duration_since(self.last_cleanup) >= CLEANUP_INTERVAL {
            self.cleanup_expired(now);
        }
    }

    fn remove(&mut self, key: &FragmentKey) {
        if let Some(assembly) = self.assemblies.remove(key) {
            self.total_buffered = self.total_buffered.saturating_sub(assembly.buffered);
        }
    }
}

fn validate_fragment(fragment: &ParsedIpFragment<'_>) -> Option<FragmentRejectReason> {
    let Some(end) = fragment.offset.checked_add(fragment.payload.len()) else {
        return Some(FragmentRejectReason::InvalidLength);
    };
    if fragment.payload.is_empty()
        || end > MAX_REASSEMBLED_PAYLOAD_BYTES
        || (fragment.more_fragments && !fragment.payload.len().is_multiple_of(8))
    {
        return Some(FragmentRejectReason::InvalidLength);
    }
    None
}

fn ranges_overlap(
    left_start: usize,
    left_end: usize,
    right_start: usize,
    right_end: usize,
) -> bool {
    left_start < right_end && right_start < left_end
}
