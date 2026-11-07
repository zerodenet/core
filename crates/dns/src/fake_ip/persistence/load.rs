//! One bounded line/value at a time; preserve v1/v2 collision semantics.
use super::*;
use std::io::{BufRead, BufReader, Read};
#[derive(Clone, Copy)]
enum Version {
    V1,
    V2,
    V3,
    Unknown,
}
pub(super) fn load(
    journal: &JournalGrant,
    expected: &PersistenceMetadata,
    now: u64,
) -> io::Result<LoadResult> {
    journal
        .read_snapshot(|reader| parse(reader, expected, now))
        .map(|loaded| loaded.unwrap_or(LoadResult::Missing))
}
fn parse(
    reader: &mut dyn Read,
    expected: &PersistenceMetadata,
    now: u64,
) -> io::Result<LoadResult> {
    let mut lines = Lines {
        reader: BufReader::new(reader),
        line: Vec::new(),
        number: 0,
    };
    let first = match lines.next_value() {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Ok(LoadResult::Corrupt(if lines.number == 0 {
                "state file is empty".into()
            } else {
                "state file does not start with a header".into()
            }))
        }
        Err(error) => return corrupt_or_error(error),
    };
    let missing_schema = first
        .get("schema")
        .and_then(serde_json::Value::as_str)
        .is_none();
    let version = match first.get("schema").and_then(serde_json::Value::as_str) {
        Some(SCHEMA) => Version::V3,
        Some(DUAL_STACK_SCHEMA) => Version::V2,
        Some(LEGACY_SCHEMA) => Version::V1,
        _ => Version::Unknown,
    };
    let compatible = (|| -> io::Result<bool> {
        Ok(match version {
            Version::V1 => {
                matches!(serde_json::from_value::<LegacyJournalRecord>(first).map_err(invalid)?,
            LegacyJournalRecord::Header { schema, metadata } if schema == LEGACY_SCHEMA
                && metadata.cidr == expected.cidr && metadata.ttl_seconds == expected.ttl_seconds
                && metadata.max_entries == expected.max_entries && metadata.exclusions == expected.exclusions)
            }
            Version::V2 | Version::V3 => {
                matches!(serde_json::from_value::<JournalRecord>(first).map_err(invalid)?,
            JournalRecord::Header { metadata, .. } if metadata == *expected)
            }
            Version::Unknown => false,
        })
    })();
    let (compatible, mut typed_error) = match compatible {
        Ok(value) => (value, None),
        Err(error) => (false, Some(error)),
    };
    if !compatible {
        // The previous implementation validated every JSON line before deciding
        // header compatibility. Preserve that distinction without collecting it.
        while let Some(value) = match lines.next_value() {
            Ok(value) => value,
            Err(error) => return corrupt_or_error(error),
        } {
            let result = match version {
                Version::V1 => serde_json::from_value::<LegacyJournalRecord>(value)
                    .map(|_| ())
                    .map_err(invalid),
                Version::V2 | Version::V3 => serde_json::from_value::<JournalRecord>(value)
                    .map(|_| ())
                    .map_err(invalid),
                Version::Unknown => Ok(()),
            };
            if let Err(error) = result {
                typed_error.get_or_insert(error);
            }
        }
        if let Some(error) = typed_error {
            return Err(error);
        }
        return Ok(if missing_schema {
            LoadResult::Corrupt("state file does not start with a header".into())
        } else {
            LoadResult::Incompatible
        });
    }
    let records = Records {
        lines,
        version,
        ended: false,
    };
    match normalize::normalize_records(
        records,
        expected.max_entries,
        now,
        expected.ttl_seconds.saturating_mul(1_000),
    ) {
        Ok(loaded) => Ok(loaded),
        Err(error) => corrupt_or_error(error),
    }
}
fn invalid(error: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
#[derive(Debug)]
struct Syntax {
    line: usize,
    source: serde_json::Error,
}
impl std::fmt::Display for Syntax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid record {}: {}", self.line, self.source)
    }
}
impl std::error::Error for Syntax {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
pub(super) fn syntax_error(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|source| source.is::<Syntax>())
}
fn corrupt_or_error(error: io::Error) -> io::Result<LoadResult> {
    if let Some(source) = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Syntax>())
    {
        return Ok(LoadResult::Corrupt(source.to_string()));
    }
    Err(error)
}
struct Lines<'a> {
    reader: BufReader<&'a mut dyn Read>,
    line: Vec<u8>,
    number: usize,
}
impl Lines<'_> {
    fn next_value(&mut self) -> io::Result<Option<serde_json::Value>> {
        loop {
            self.line.clear();
            let count = self.reader.read_until(b'\n', &mut self.line)?;
            if count == 0 {
                return Ok(None);
            }
            self.number += 1;
            let terminated = self.line.last() == Some(&b'\n');
            if terminated {
                self.line.pop();
            }
            if self.line.is_empty() {
                continue;
            }
            match serde_json::from_slice(&self.line) {
                Ok(value) => return Ok(Some(value)),
                Err(_) if !terminated => {
                    tracing::warn!("ignoring incomplete trailing Fake-IP journal record");
                    return Ok(None);
                }
                Err(source) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        Syntax {
                            line: self.number,
                            source,
                        },
                    ))
                }
            }
        }
    }
}
struct Records<'a> {
    lines: Lines<'a>,
    version: Version,
    ended: bool,
}
impl Iterator for Records<'_> {
    type Item = io::Result<PersistedRecord>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        let value = match self.lines.next_value() {
            Ok(Some(value)) => value,
            Ok(None) => {
                self.ended = true;
                return None;
            }
            Err(error) => {
                self.ended = true;
                return Some(Err(error));
            }
        };
        Some(match self.version {
            Version::V1 => serde_json::from_value::<LegacyJournalRecord>(value)
                .map_err(invalid)
                .and_then(|record| match record {
                    LegacyJournalRecord::Upsert {
                        domain,
                        ip,
                        expires_at_unix_ms,
                    } => Ok(PersistedRecord::Mapping(PersistedMapping {
                        domain,
                        ip: IpAddr::V4(Ipv4Addr::from(ip)),
                        expires_at_unix_ms,
                    })),
                    LegacyJournalRecord::Header { .. } => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Fake-IP state contains an unexpected legacy header",
                    )),
                }),
            Version::V2 | Version::V3 => serde_json::from_value::<JournalRecord>(value)
                .map_err(invalid)
                .and_then(|record| match record {
                    JournalRecord::Upsert {
                        domain,
                        ip,
                        expires_at_unix_ms,
                    } => Ok(PersistedRecord::Mapping(PersistedMapping {
                        domain,
                        ip,
                        expires_at_unix_ms,
                    })),
                    JournalRecord::Retire {
                        ip,
                        reusable_after_unix_ms,
                    } if matches!(self.version, Version::V3) => {
                        Ok(PersistedRecord::Retired(PersistedRetiredAddress {
                            ip,
                            reusable_after_unix_ms,
                        }))
                    }
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Fake-IP state contains an unexpected record",
                    )),
                }),
            Version::Unknown => unreachable!("unknown schema never supplies domain records"),
        })
    }
}
