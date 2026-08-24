use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const SCHEMA: &str = "zero.dns.fake-ip.v1";
const COMPACT_MIN_RECORDS: usize = 1_024;
const COMPACT_MAX_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistenceMetadata {
    pub(super) cidr: String,
    pub(super) ttl_seconds: u64,
    pub(super) max_entries: usize,
    pub(super) exclusions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PersistedMapping {
    pub(super) domain: String,
    pub(super) ip: [u8; 4],
    pub(super) expires_at_unix_ms: u64,
}

pub(super) struct FakeIpPersistence {
    lease: Arc<FakeIpStateLease>,
    generation: u64,
}

#[derive(Debug)]
pub(crate) struct FakeIpStateLease {
    path: PathBuf,
    _lock: File,
    journal: Mutex<JournalState>,
}

#[derive(Debug, Default)]
struct JournalState {
    generation: u64,
    file: Option<File>,
    metadata: Option<PersistenceMetadata>,
    records: usize,
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
        ip: [u8; 4],
        expires_at_unix_ms: u64,
    },
}

enum LoadResult {
    Missing,
    Compatible(Vec<PersistedMapping>),
    Incompatible,
    Corrupt(String),
}

impl FakeIpPersistence {
    pub(super) fn open(
        lease: Arc<FakeIpStateLease>,
        metadata: PersistenceMetadata,
    ) -> io::Result<(Self, Vec<PersistedMapping>)> {
        let path = lease.path.clone();
        create_private_parent(&path)?;
        let mut journal = lease
            .journal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = unix_time_ms()?;
        let loaded = load(&path, &metadata, now)?;
        journal.file.take();
        let recovered = match loaded {
            LoadResult::Missing => Vec::new(),
            LoadResult::Compatible(mappings) => mappings,
            LoadResult::Incompatible => {
                tracing::info!(
                    path = %path.display(),
                    "discarding incompatible Fake-IP persistence state"
                );
                Vec::new()
            }
            LoadResult::Corrupt(error) => {
                let quarantined = quarantine_corrupt_file(&path);
                tracing::warn!(
                    path = %path.display(),
                    quarantined = quarantined.as_ref().map(|path| path.display().to_string()),
                    %error,
                    "discarding corrupt Fake-IP persistence state"
                );
                Vec::new()
            }
        };

        if let Err(error) = rewrite(&path, &metadata, &recovered) {
            journal.file = open_append(&path).ok();
            return Err(error);
        }
        let file = open_append(&path)?;
        let generation = journal.generation.wrapping_add(1);
        journal.generation = generation;
        journal.file = Some(file);
        journal.metadata = Some(metadata);
        journal.records = recovered.len() + 1;
        drop(journal);
        Ok((Self { lease, generation }, recovered))
    }

    pub(super) fn append_upsert(&mut self, mapping: &PersistedMapping) -> io::Result<()> {
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        ensure_generation(&journal, self.generation)?;
        let record = JournalRecord::Upsert {
            domain: mapping.domain.clone(),
            ip: mapping.ip,
            expires_at_unix_ms: mapping.expires_at_unix_ms,
        };
        let file = journal.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "Fake-IP journal is not open")
        })?;
        write_record(file, &record)?;
        file.flush()?;
        journal.records = journal.records.saturating_add(1);
        Ok(())
    }

    pub(super) fn should_compact(&self, live_mappings: usize) -> bool {
        let journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        journal.generation == self.generation
            && (journal.records
                > COMPACT_MIN_RECORDS.max(live_mappings.saturating_mul(4).saturating_add(1))
                || journal
                    .file
                    .as_ref()
                    .and_then(|file| file.metadata().ok())
                    .is_some_and(|metadata| metadata.len() > COMPACT_MAX_BYTES))
    }

    pub(super) fn compact(&mut self, mappings: &[PersistedMapping]) -> io::Result<()> {
        let mut journal = self
            .lease
            .journal
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        ensure_generation(&journal, self.generation)?;
        let metadata = journal.metadata.clone().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Fake-IP journal has no header")
        })?;
        journal.file.take();
        if let Err(error) = rewrite(&self.lease.path, &metadata, mappings) {
            journal.file = open_append(&self.lease.path).ok();
            return Err(error);
        }
        journal.file = Some(open_append(&self.lease.path)?);
        journal.records = mappings.len() + 1;
        Ok(())
    }
}

impl FakeIpStateLease {
    pub(crate) fn acquire(path: PathBuf) -> io::Result<Arc<Self>> {
        create_private_parent(&path)?;
        let lock_path = path.with_extension("lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|error| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "Fake-IP state `{}` is already owned by another Zero process (`{}`): {error}",
                    path.display(),
                    lock_path.display()
                ),
            )
        })?;
        Ok(Arc::new(Self {
            path,
            _lock: lock,
            journal: Mutex::new(JournalState::default()),
        }))
    }
}

fn ensure_generation(journal: &JournalState, generation: u64) -> io::Result<()> {
    if journal.generation == generation {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "Fake-IP allocator was replaced by a configuration reload",
        ))
    }
}

pub(super) fn unix_time_ms() -> io::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| io::Error::other(format!("system clock is before UNIX epoch: {error}")))?
        .as_millis();
    Ok(millis.min(u128::from(u64::MAX)) as u64)
}

fn load(path: &Path, expected: &PersistenceMetadata, now_unix_ms: u64) -> io::Result<LoadResult> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(LoadResult::Missing),
        Err(error) => return Err(error),
    };
    if raw.is_empty() {
        return Ok(LoadResult::Corrupt("state file is empty".to_owned()));
    }

    let ends_with_newline = raw.ends_with(b"\n");
    let mut records = Vec::new();
    for (index, line) in raw.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<JournalRecord>(line) {
            Ok(record) => records.push(record),
            Err(_error)
                if !ends_with_newline && index == raw.split(|byte| *byte == b'\n').count() - 1 =>
            {
                tracing::warn!(
                    path = %path.display(),
                    "ignoring incomplete trailing Fake-IP journal record"
                );
            }
            Err(error) => {
                return Ok(LoadResult::Corrupt(format!(
                    "invalid record {}: {error}",
                    index + 1
                )));
            }
        }
    }

    let Some(JournalRecord::Header { schema, metadata }) = records.first() else {
        return Ok(LoadResult::Corrupt(
            "state file does not start with a header".to_owned(),
        ));
    };
    if schema != SCHEMA || metadata != expected {
        return Ok(LoadResult::Incompatible);
    }

    let mut by_domain = std::collections::HashMap::<String, [u8; 4]>::new();
    let mut by_ip = std::collections::HashMap::<[u8; 4], (usize, PersistedMapping)>::new();
    for (sequence, record) in records.into_iter().skip(1).enumerate() {
        let JournalRecord::Upsert {
            domain,
            ip,
            expires_at_unix_ms,
        } = record
        else {
            return Ok(LoadResult::Corrupt(
                "state file contains an unexpected header".to_owned(),
            ));
        };
        if let Some(old_ip) = by_domain.remove(&domain) {
            by_ip.remove(&old_ip);
        }
        if let Some((_, old_mapping)) = by_ip.remove(&ip) {
            by_domain.remove(&old_mapping.domain);
        }
        if expires_at_unix_ms > now_unix_ms {
            by_domain.insert(domain.clone(), ip);
            by_ip.insert(
                ip,
                (
                    sequence,
                    PersistedMapping {
                        domain,
                        ip,
                        expires_at_unix_ms,
                    },
                ),
            );
        }
    }
    let mut mappings = by_ip.into_values().collect::<Vec<_>>();
    mappings.sort_by_key(|(sequence, _)| *sequence);
    let mappings = mappings
        .into_iter()
        .map(|(_, mapping)| mapping)
        .collect::<Vec<_>>();
    if mappings.len() > expected.max_entries {
        return Ok(LoadResult::Corrupt(format!(
            "state contains {} mappings but capacity is {}",
            mappings.len(),
            expected.max_entries
        )));
    }
    Ok(LoadResult::Compatible(mappings))
}

fn rewrite(
    path: &Path,
    metadata: &PersistenceMetadata,
    mappings: &[PersistedMapping],
) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    write_record(
        temporary.as_file_mut(),
        &JournalRecord::Header {
            schema: SCHEMA.to_owned(),
            metadata: metadata.clone(),
        },
    )?;
    for mapping in mappings {
        write_record(
            temporary.as_file_mut(),
            &JournalRecord::Upsert {
                domain: mapping.domain.clone(),
                ip: mapping.ip,
                expires_at_unix_ms: mapping.expires_at_unix_ms,
            },
        )?;
    }
    temporary.as_file_mut().flush()?;
    temporary.as_file_mut().sync_all()?;
    persist_temporary(temporary, path)
}

fn write_record(writer: &mut impl Write, record: &JournalRecord) -> io::Result<()> {
    let encoded = serde_json::to_vec(record)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    writer.write_all(&encoded)?;
    writer.write_all(b"\n")
}

fn quarantine_corrupt_file(path: &Path) -> Option<PathBuf> {
    let timestamp = unix_time_ms().ok()?;
    let file_name = path.file_name()?.to_string_lossy();
    let quarantined = path.with_file_name(format!("{file_name}.corrupt-{timestamp}"));
    std::fs::rename(path, &quarantined).ok()?;
    Some(quarantined)
}

fn create_private_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn open_append(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).append(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        // Preserve read/write sharing and additionally allow an incompatible
        // hot reload to atomically replace the journal while in-flight DNS
        // requests still hold the previous allocator open.
        options.share_mode(0x1 | 0x2 | 0x4);
    }
    options.open(path)
}

#[cfg(not(windows))]
fn persist_temporary(temporary: tempfile::NamedTempFile, path: &Path) -> io::Result<()> {
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

#[cfg(windows)]
fn persist_temporary(temporary: tempfile::NamedTempFile, path: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let temporary = temporary.into_temp_path();
    let source = temporary
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
