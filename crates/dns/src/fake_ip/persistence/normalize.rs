//! Same collision/quarantine normalization, consuming one decoded record.
use super::*;
pub(super) fn normalize_records(
    records: impl IntoIterator<Item = io::Result<PersistedRecord>>,
    max_entries: usize,
    now_unix_ms: u64,
    retirement_ttl_ms: u64,
) -> io::Result<LoadResult> {
    let mut by_domain_family = HashMap::<(String, bool), IpAddr>::new();
    let mut by_ip = HashMap::<IpAddr, (usize, PersistedMapping)>::new();
    let mut retired = HashMap::<IpAddr, (usize, PersistedRetiredAddress)>::new();
    let mut retired_owner = HashMap::<IpAddr, Option<String>>::new();
    let mut first_error = None;
    for (sequence, record) in records.into_iter().enumerate() {
        let record = match record {
            Ok(record) if first_error.is_none() => record,
            Ok(_) => continue,
            Err(error) => {
                // The original reader validated all JSON syntax before typed
                // decoding. Drain without retaining more rows to preserve that
                // error priority in this bounded streaming implementation.
                if load::syntax_error(&error) || first_error.is_none() {
                    first_error = Some(error);
                }
                continue;
            }
        };
        match record {
            PersistedRecord::Mapping(mapping) => {
                let family_key = (mapping.domain.clone(), mapping.ip.is_ipv6());
                if let Some(old_ip) = by_domain_family.remove(&family_key) {
                    if old_ip != mapping.ip {
                        by_ip.remove(&old_ip);
                        let retired_address = PersistedRetiredAddress {
                            ip: old_ip,
                            reusable_after_unix_ms: now_unix_ms.saturating_add(retirement_ttl_ms),
                        };
                        retired.insert(old_ip, (sequence, retired_address));
                        retired_owner.insert(old_ip, Some(mapping.domain.clone()));
                    }
                }
                if let Some((_, old_mapping)) = by_ip.remove(&mapping.ip) {
                    let changed_owner = old_mapping.domain != mapping.domain;
                    by_domain_family.remove(&(old_mapping.domain, old_mapping.ip.is_ipv6()));
                    if changed_owner {
                        let retired_address = PersistedRetiredAddress {
                            ip: mapping.ip,
                            reusable_after_unix_ms: now_unix_ms.saturating_add(retirement_ttl_ms),
                        };
                        retired.insert(mapping.ip, (sequence, retired_address));
                        retired_owner.insert(mapping.ip, None);
                        continue;
                    }
                }
                if let Entry::Occupied(mut entry) = retired.entry(mapping.ip) {
                    let same_owner = retired_owner
                        .get(&mapping.ip)
                        .and_then(|owner| owner.as_deref())
                        == Some(mapping.domain.as_str());
                    if same_owner {
                        entry.remove();
                        retired_owner.remove(&mapping.ip);
                    } else {
                        let retired_address = PersistedRetiredAddress {
                            ip: mapping.ip,
                            reusable_after_unix_ms: now_unix_ms.saturating_add(retirement_ttl_ms),
                        };
                        entry.insert((sequence, retired_address));
                        retired_owner.insert(mapping.ip, None);
                        continue;
                    }
                }
                if mapping.expires_at_unix_ms > now_unix_ms {
                    by_domain_family.insert(family_key, mapping.ip);
                    by_ip.insert(mapping.ip, (sequence, mapping));
                } else {
                    let retired_address = PersistedRetiredAddress {
                        ip: mapping.ip,
                        reusable_after_unix_ms: mapping
                            .expires_at_unix_ms
                            .saturating_add(retirement_ttl_ms),
                    };
                    if retired_address.reusable_after_unix_ms > now_unix_ms {
                        retired.insert(mapping.ip, (sequence, retired_address));
                        retired_owner.insert(mapping.ip, Some(mapping.domain));
                    }
                }
            }
            PersistedRecord::Retired(address) => {
                if let Some((_, old_mapping)) = by_ip.remove(&address.ip) {
                    by_domain_family.remove(&(old_mapping.domain, old_mapping.ip.is_ipv6()));
                }
                retired.remove(&address.ip);
                retired_owner.remove(&address.ip);
                if address.reusable_after_unix_ms > now_unix_ms {
                    retired.insert(address.ip, (sequence, address));
                    retired_owner.insert(address.ip, None);
                }
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let domains = by_ip
        .values()
        .map(|(_, mapping)| mapping.domain.as_str())
        .collect::<HashSet<_>>();
    if domains.len() > max_entries {
        return Ok(LoadResult::Corrupt(format!(
            "state contains {} domains but capacity is {max_entries}",
            domains.len()
        )));
    }
    let mut mappings = by_ip.into_values().collect::<Vec<_>>();
    mappings.sort_by_key(|(sequence, _)| *sequence);
    let mut retired = retired.into_values().collect::<Vec<_>>();
    retired.sort_by_key(|(sequence, _)| *sequence);
    Ok(LoadResult::Compatible(PersistedState {
        mappings: mappings.into_iter().map(|(_, mapping)| mapping).collect(),
        retired: retired.into_iter().map(|(_, address)| address).collect(),
    }))
}
