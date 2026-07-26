use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

use serde::Serialize;
use zero_config::RuntimeConfig;

use crate::dispatcher::outbox;
use crate::{ConnectorError, ConnectorResult};

pub const CONNECTOR_STATE_REPORT_SCHEMA: &str = "zero.connector.state-report.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectorStateReport {
    pub schema_id: &'static str,
    pub compatible: bool,
    pub files: Vec<ConnectorStateFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectorStateFile {
    pub kind: String,
    pub format: String,
    pub path: String,
    pub status: ConnectorStateStatus,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorStateStatus {
    Missing,
    Ready,
    RecoverablePartialTail,
    Incompatible,
}

#[derive(Debug)]
pub(crate) struct PersistentStateLease {
    _file: File,
}

impl PersistentStateLease {
    pub(crate) fn acquire(path: &Path) -> ConnectorResult<Self> {
        let lock_path = lock_path(path);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| {
                ConnectorError::LockPersistentState {
                    path: path.display().to_string(),
                    lock_path: lock_path.display().to_string(),
                    source,
                }
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&lock_path)
            .map_err(|source| ConnectorError::LockPersistentState {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
                source,
            })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(ConnectorError::PersistentStateInUse {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
            }),
            Err(TryLockError::Error(source)) => Err(ConnectorError::LockPersistentState {
                path: path.display().to_string(),
                lock_path: lock_path.display().to_string(),
                source,
            }),
        }
    }
}

fn lock_path(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    PathBuf::from(lock_path)
}

impl ConnectorStateReport {
    pub fn is_compatible(&self) -> bool {
        self.compatible
    }
}

pub fn inspect_persistent_state(config: &RuntimeConfig) -> ConnectorStateReport {
    let mut files = Vec::new();
    if let Some(path) = config.api.outbox_path.as_deref() {
        files.push(inspect_outbox_file(
            "event_outbox",
            path,
            config.source_dir(),
        ));
    }
    if let Some(path) = config.api.dead_letter_path.as_deref() {
        files.push(inspect_json_lines(
            "dead_letter",
            "zero.connector.dead-letter-jsonl.v1",
            path,
            config.source_dir(),
        ));
    }
    let compatible = files
        .iter()
        .all(|file| file.status != ConnectorStateStatus::Incompatible);
    ConnectorStateReport {
        schema_id: CONNECTOR_STATE_REPORT_SCHEMA,
        compatible,
        files,
    }
}

pub(crate) fn inspect_outbox_file(
    kind: &str,
    configured_path: &str,
    source_dir: Option<&Path>,
) -> ConnectorStateFile {
    let path = resolve_path(configured_path, source_dir);
    if !path.exists() {
        return missing(kind, "zero.connector.delivery-outbox.v1", &path);
    }
    match outbox::inspect_path(&path) {
        Ok(inspection) => ConnectorStateFile {
            kind: kind.to_owned(),
            format: "zero.connector.delivery-outbox.v1".to_owned(),
            path: path.display().to_string(),
            status: if inspection.recoverable_partial_tail {
                ConnectorStateStatus::RecoverablePartialTail
            } else {
                ConnectorStateStatus::Ready
            },
            bytes: inspection.bytes,
            records: Some(inspection.records as u64),
            pending: Some(inspection.pending as u64),
            error: None,
        },
        Err(error) => incompatible(
            kind,
            "zero.connector.delivery-outbox.v1",
            &path,
            file_len(&path),
            error.to_string(),
        ),
    }
}

fn inspect_json_lines(
    kind: &str,
    format: &str,
    configured_path: &str,
    source_dir: Option<&Path>,
) -> ConnectorStateFile {
    let path = resolve_path(configured_path, source_dir);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return missing(kind, format, &path);
        }
        Err(error) => {
            return incompatible(kind, format, &path, 0, format!("read failed: {error}"));
        }
    };
    let mut records = 0u64;
    for (index, line) in raw.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        if let Err(error) = serde_json::from_slice::<serde_json::Value>(line) {
            return incompatible(
                kind,
                format,
                &path,
                raw.len() as u64,
                format!("record {} is invalid JSON: {error}", index + 1),
            );
        }
        records += 1;
    }
    ConnectorStateFile {
        kind: kind.to_owned(),
        format: format.to_owned(),
        path: path.display().to_string(),
        status: ConnectorStateStatus::Ready,
        bytes: raw.len() as u64,
        records: Some(records),
        pending: None,
        error: None,
    }
}

fn missing(kind: &str, format: &str, path: &Path) -> ConnectorStateFile {
    ConnectorStateFile {
        kind: kind.to_owned(),
        format: format.to_owned(),
        path: path.display().to_string(),
        status: ConnectorStateStatus::Missing,
        bytes: 0,
        records: None,
        pending: None,
        error: None,
    }
}

fn incompatible(
    kind: &str,
    format: &str,
    path: &Path,
    bytes: u64,
    error: String,
) -> ConnectorStateFile {
    ConnectorStateFile {
        kind: kind.to_owned(),
        format: format.to_owned(),
        path: path.display().to_string(),
        status: ConnectorStateStatus::Incompatible,
        bytes,
        records: None,
        pending: None,
        error: Some(error),
    }
}

fn resolve_path(path: &str, source_dir: Option<&Path>) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        source_dir.unwrap_or_else(|| Path::new(".")).join(path)
    }
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}
