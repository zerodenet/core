//! Persistent Fake-IP format; native custody belongs to its journal grant.
use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use zero_platform_tokio::journal::JournalGrant;
mod change;
pub(super) mod codec;
mod lease;
mod load;
mod normalize;
use codec::{rewrite, write_record};
pub(crate) use lease::FakeIpStateLease;
pub(crate) use lease::JournalPause;
pub use lease::{FakeIpStateOpenFailure, JournalGenerationExhausted};
pub(super) const SCHEMA: &str = "zero.dns.fake-ip.v3";
const DUAL_STACK_SCHEMA: &str = "zero.dns.fake-ip.v2";
const LEGACY_SCHEMA: &str = "zero.dns.fake-ip.v1";
const COMPACT_MIN_RECORDS: usize = 1_024;
const COMPACT_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistenceMetadata {
    pub(super) cidr: String,
    #[serde(default)]
    pub(super) ipv6_cidr: Option<String>,
    pub(super) ttl_seconds: u64,
    pub(super) max_entries: usize,
    pub(super) exclusions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PersistedMapping {
    pub(super) domain: String,
    pub(super) ip: IpAddr,
    pub(super) expires_at_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PersistedRetiredAddress {
    pub(super) ip: IpAddr,
    pub(super) reusable_after_unix_ms: u64,
}

#[derive(Debug, Default)]
pub(super) struct PersistedState {
    pub(super) mappings: Vec<PersistedMapping>,
    pub(super) retired: Vec<PersistedRetiredAddress>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum JournalRecord {
    Header {
        schema: String,
        #[serde(flatten)]
        metadata: PersistenceMetadata,
    },
    Upsert {
        domain: String,
        ip: IpAddr,
        expires_at_unix_ms: u64,
    },
    Retire {
        ip: IpAddr,
        reusable_after_unix_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyMetadata {
    cidr: String,
    ttl_seconds: u64,
    max_entries: usize,
    exclusions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum LegacyJournalRecord {
    Header {
        schema: String,
        #[serde(flatten)]
        metadata: LegacyMetadata,
    },
    Upsert {
        domain: String,
        ip: [u8; 4],
        expires_at_unix_ms: u64,
    },
}

enum LoadResult {
    Missing,
    Compatible(PersistedState),
    Incompatible,
    Corrupt(String),
}

enum PersistedRecord {
    Mapping(PersistedMapping),
    Retired(PersistedRetiredAddress),
}

pub(super) struct FakeIpPersistence {
    lease: Arc<FakeIpStateLease>,
    generation: u64,
    metadata: PersistenceMetadata,
}
impl FakeIpPersistence {
    pub(super) fn open(
        lease: Arc<FakeIpStateLease>,
        metadata: PersistenceMetadata,
    ) -> io::Result<(Self, PersistedState)> {
        if !lease.bound() {
            // Production cold construction opens nothing. Actual Default owner
            // activation supplies its sole declared grant before queries open.
            return Ok((
                Self {
                    lease,
                    generation: 0,
                    metadata,
                },
                PersistedState::default(),
            ));
        }
        let _call = lease.begin()?;
        let grant = lease.grant()?;
        let journal = lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = journal
            .generation
            .checked_add(1)
            .ok_or_else(lease::generation_exhausted)?;
        if !lease.matches(&grant) {
            return Err(stale());
        }
        drop(journal);
        let recovered = match load::load(&grant, &metadata, unix_time_ms()?)? {
            LoadResult::Missing => PersistedState::default(),
            LoadResult::Compatible(state) => state,
            LoadResult::Incompatible => {
                tracing::info!(path = %lease.path.display(), "discarding incompatible Fake-IP persistence state");
                PersistedState::default()
            }
            LoadResult::Corrupt(error) => {
                let preserved = grant.preserve_previous(&format!("corrupt-{}", unix_time_ms()?))?;
                tracing::warn!(path = %lease.path.display(), preserved = ?preserved, %error, "discarding corrupt Fake-IP persistence state");
                PersistedState::default()
            }
        };
        let bytes = rewrite(&grant, &metadata, &recovered.mappings, &recovered.retired)?;
        let mut journal = lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.generation = generation;
        journal.ready = true;
        journal.bytes = bytes;
        journal.records = recovered.mappings.len() + recovered.retired.len() + 1;
        drop(journal);
        drop(_call);
        Ok((
            Self {
                lease,
                generation,
                metadata,
            },
            recovered,
        ))
    }
    pub(super) fn ensure_next_generation(&self) -> io::Result<()> {
        self.lease.ensure_next_generation()
    }
    pub(super) fn paused(&self) -> bool {
        self.lease.paused()
    }
    pub(super) fn pause(&self) -> io::Result<JournalPause> {
        self.lease.pause()
    }
    pub(super) fn bytes(&self) -> usize {
        self.lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bytes
    }
    pub(super) fn initialized(&self) -> bool {
        self.generation != 0
    }
    pub(super) fn activation_needed(&self) -> bool {
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !journal.ready || self.generation != journal.generation
    }
    pub(super) fn reopen(&self) -> io::Result<(Self, PersistedState)> {
        Self::open(self.lease.clone(), self.metadata.clone())
    }
    pub(super) fn should_compact(&self, live_records: usize) -> bool {
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.ready
            && journal.generation == self.generation
            && (journal.records
                > COMPACT_MIN_RECORDS.max(live_records.saturating_mul(8).saturating_add(1))
                || journal.bytes > COMPACT_MAX_BYTES)
    }
    pub(super) fn compact(
        &mut self,
        mappings: &[PersistedMapping],
        retired: &[PersistedRetiredAddress],
    ) -> io::Result<()> {
        let _call = self.lease.begin()?;
        let grant = self.lease.grant()?;
        {
            let journal = self
                .lease
                .journal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.ensure_current(&journal, &grant)?;
        }
        let bytes = rewrite(&grant, &self.metadata, mappings, retired)?;
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.bytes = bytes;
        journal.records = mappings.len() + retired.len() + 1;
        Ok(())
    }
    fn ensure_current(
        &self,
        journal: &lease::JournalState,
        grant: &JournalGrant,
    ) -> io::Result<()> {
        if !journal.ready
            || self.generation == 0
            || journal.generation != self.generation
            || !self.lease.matches(grant)
        {
            return Err(stale());
        }
        Ok(())
    }
}
fn stale() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "Fake-IP journal generation is not the active original",
    )
}
pub(super) fn unix_time_ms() -> io::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    u64::try_from(duration.as_millis()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Fake-IP clock exceeds its persistent representation",
        )
    })
}

#[cfg(test)]
mod tests;
