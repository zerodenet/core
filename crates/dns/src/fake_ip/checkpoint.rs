//! Borrowed complete durable state, fixed under the actual allocator guard.
//! Only a bounded pointer workspace is sorted; domains are never cloned.
use super::*;
use persistence::codec::{self, RecordRef};
use std::io::Write;
pub(super) struct Upsert<'a> {
    pub domain: &'a str,
    pub addresses: &'a [IpAddr],
    pub expires: u64,
}
pub(super) struct Retire<'a> {
    pub domain: &'a str,
    pub addresses: &'a [IpAddr],
    pub reusable_after: u64,
}
pub(super) struct Checkpoint<'a> {
    rows: Vec<(&'a String, &'a Mapping)>,
    retired: &'a HashMap<IpAddr, RetiredAddress>,
    upsert: Option<Upsert<'a>>,
    retire: Option<Retire<'a>>,
    extra_retired: Vec<(IpAddr, u64)>,
}
impl<'a> Checkpoint<'a> {
    pub fn new(
        state: &'a AllocatorState,
        upsert: Option<Upsert<'a>>,
        retire: Option<Retire<'a>>,
    ) -> io::Result<Self> {
        Self::from_maps(&state.forward, &state.retired, upsert, retire)
    }
    pub fn from_maps(
        forward: &'a HashMap<String, Mapping>,
        retired: &'a HashMap<IpAddr, RetiredAddress>,
        upsert: Option<Upsert<'a>>,
        retire: Option<Retire<'a>>,
    ) -> io::Result<Self> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(forward.len())
            .map_err(io::Error::other)?;
        rows.extend(forward.iter().filter(|(domain, _)| {
            !upsert
                .as_ref()
                .is_some_and(|update| update.domain == domain.as_str())
                && !retire
                    .as_ref()
                    .is_some_and(|update| update.domain == domain.as_str())
        }));
        rows.sort_by_key(|(_, mapping)| mapping.last_used);
        Ok(Self {
            rows,
            retired,
            upsert,
            retire,
            extra_retired: Vec::new(),
        })
    }
    pub fn encode(&self, metadata: &PersistenceMetadata, writer: &mut dyn Write) -> io::Result<()> {
        codec::write_record(
            writer,
            &RecordRef::Header {
                schema: persistence::SCHEMA,
                metadata,
            },
        )?;
        for (domain, mapping) in &self.rows {
            for ip in mapping.addresses() {
                codec::write_record(
                    writer,
                    &RecordRef::Upsert {
                        domain,
                        ip,
                        expires_at_unix_ms: mapping.expires_at_unix_ms,
                    },
                )?;
            }
        }
        if let Some(update) = &self.upsert {
            for ip in update.addresses {
                codec::write_record(
                    writer,
                    &RecordRef::Upsert {
                        domain: update.domain,
                        ip: *ip,
                        expires_at_unix_ms: update.expires,
                    },
                )?;
            }
        }
        for (ip, address) in self.retired {
            if !self
                .retire
                .as_ref()
                .is_some_and(|update| update.addresses.contains(ip))
            {
                codec::write_record(
                    writer,
                    &RecordRef::Retire {
                        ip: *ip,
                        reusable_after_unix_ms: address.reusable_after_unix_ms,
                    },
                )?;
            }
        }
        if let Some(update) = &self.retire {
            for ip in update.addresses {
                codec::write_record(
                    writer,
                    &RecordRef::Retire {
                        ip: *ip,
                        reusable_after_unix_ms: update.reusable_after,
                    },
                )?;
            }
        }
        for (ip, reusable_after_unix_ms) in &self.extra_retired {
            codec::write_record(
                writer,
                &RecordRef::Retire {
                    ip: *ip,
                    reusable_after_unix_ms: *reusable_after_unix_ms,
                },
            )?;
        }
        Ok(())
    }
    pub fn records(&self) -> usize {
        1 + self.extra_retired.len()
            + self
                .rows
                .iter()
                .map(|(_, row)| row.addresses().count())
                .sum::<usize>()
            + self
                .upsert
                .as_ref()
                .map_or(0, |update| update.addresses.len())
            + self
                .retired
                .keys()
                .filter(|ip| {
                    !self
                        .retire
                        .as_ref()
                        .is_some_and(|update| update.addresses.contains(ip))
                })
                .count()
            + self
                .retire
                .as_ref()
                .map_or(0, |update| update.addresses.len())
    }
}

pub(super) fn persist_allocation(
    state: &AllocatorState,
    domain: &str,
    addresses: &[IpAddr],
    expires: u64,
    victim: Option<&str>,
    reusable_after: u64,
) -> io::Result<bool> {
    let Some(persistence) = &state.persistence else {
        return Ok(false);
    };
    let old_addresses = victim
        .and_then(|domain| state.forward.get(domain))
        .map(|mapping| mapping.addresses().collect::<Vec<_>>())
        .unwrap_or_default();
    let retire = victim.map(|domain| Retire {
        domain,
        addresses: &old_addresses,
        reusable_after,
    });
    let checkpoint = Checkpoint::new(
        state,
        Some(Upsert {
            domain,
            addresses,
            expires,
        }),
        retire,
    )?;
    persistence.check_checkpoint(&checkpoint)?;
    if victim.is_some() {
        // Commit the victim retirement and complete replacement together. A
        // rejected/failed allocation cannot first evict a valid old mapping.
        persistence.compact_checkpoint(&checkpoint)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
pub(super) fn persist_upsert(
    state: &AllocatorState,
    domain: &str,
    addresses: &[IpAddr],
    expires: u64,
) -> io::Result<()> {
    let Some(persistence) = &state.persistence else {
        return Ok(());
    };
    let checkpoint = Checkpoint::new(
        state,
        Some(Upsert {
            domain,
            addresses,
            expires,
        }),
        None,
    )?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(addresses.len())
        .map_err(io::Error::other)?;
    records.extend(addresses.iter().map(|ip| RecordRef::Upsert {
        domain,
        ip: *ip,
        expires_at_unix_ms: expires,
    }));
    persistence.commit_change(&checkpoint, &records)
}
pub(super) fn persist_retire(
    state: &AllocatorState,
    domain: &str,
    addresses: &[IpAddr],
    reusable_after: u64,
) -> io::Result<()> {
    let Some(persistence) = &state.persistence else {
        return Ok(());
    };
    let checkpoint = Checkpoint::new(
        state,
        None,
        Some(Retire {
            domain,
            addresses,
            reusable_after,
        }),
    )?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(addresses.len())
        .map_err(io::Error::other)?;
    records.extend(addresses.iter().map(|ip| RecordRef::Retire {
        ip: *ip,
        reusable_after_unix_ms: reusable_after,
    }));
    persistence.commit_change(&checkpoint, &records)
}

mod clear;
