use crate::resources::{DispatcherResources, FileView};
use std::fs::OpenOptions;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use zero_api::OutboxCorruptionClass;

use crate::{ConnectorError, ConnectorResult};

pub(super) fn corruption_class(message: &str) -> OutboxCorruptionClass {
    match message.split_ascii_whitespace().next() {
        Some("malformed_tail") => OutboxCorruptionClass::MalformedTail,
        Some("malformed_middle") => OutboxCorruptionClass::MalformedMiddle,
        _ => OutboxCorruptionClass::MalformedJournal,
    }
}

pub(super) fn quarantine_journal(
    path: &Path,
    resources: &DispatcherResources,
) -> ConnectorResult<PathBuf> {
    let preserved_path = unique_preserved_path(path, "corrupt")?;
    match std::fs::rename(path, &preserved_path) {
        Ok(()) => Ok(preserved_path),
        Err(rename_error) => {
            let mut source = resources
                .open(|| std::fs::File::open(path))
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: path.display().to_string(),
                    source,
                })?;
            let mut destination = resources
                .open(|| {
                    OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&preserved_path)
                })
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: preserved_path.display().to_string(),
                    source,
                })?;
            let copied = std::io::copy(&mut source, &mut destination).map_err(|copy_error| {
                ConnectorError::OpenOutbox {
                    path: path.display().to_string(),
                    source: std::io::Error::other(format!(
                        "failed to quarantine corrupt journal by rename ({rename_error}) or copy ({copy_error})"
                    )),
                }
            })?;
            let expected = std::fs::metadata(path)
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: path.display().to_string(),
                    source,
                })?
                .len();
            if copied != expected {
                return Err(ConnectorError::OpenOutbox {
                    path: path.display().to_string(),
                    source: std::io::Error::other(format!(
                        "quarantine copy is incomplete: copied {copied} of {expected} bytes"
                    )),
                });
            }
            destination
                .sync_all()
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: preserved_path.display().to_string(),
                    source,
                })?;
            drop(source);
            drop(destination);
            resources
                .retire_files(false)
                .map_err(|source| ConnectorError::OpenOutbox {
                    path: path.display().to_string(),
                    source,
                })?;
            Ok(preserved_path)
        }
    }
}

pub(super) fn preserve_journal(
    path: &Path,
    source_file: &mut FileView,
    bytes: u64,
    resources: &DispatcherResources,
) -> ConnectorResult<PathBuf> {
    let preserved_path = unique_preserved_path(path, "partial-tail")?;
    let mut file = resources
        .open(|| {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&preserved_path)
        })
        .map_err(|source| ConnectorError::OpenOutbox {
            path: preserved_path.display().to_string(),
            source,
        })?;
    source_file
        .rewind()
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })?;
    let copied = std::io::copy(&mut source_file.take(bytes), &mut file).map_err(|source| {
        ConnectorError::OpenOutbox {
            path: preserved_path.display().to_string(),
            source,
        }
    })?;
    if copied != bytes {
        return Err(ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source: std::io::Error::other("partial-tail evidence copy is incomplete"),
        });
    }
    file.sync_all()
        .map_err(|source| ConnectorError::OpenOutbox {
            path: preserved_path.display().to_string(),
            source,
        })?;
    drop(file);
    resources
        .retire_files(false)
        .map_err(|source| ConnectorError::OpenOutbox {
            path: path.display().to_string(),
            source,
        })?;
    Ok(preserved_path)
}

fn unique_preserved_path(path: &Path, kind: &str) -> ConnectorResult<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    for attempt in 0..1_000u16 {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(format!(
            ".{kind}-{timestamp}-{}-{attempt}",
            std::process::id()
        ));
        let candidate = PathBuf::from(candidate);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(ConnectorError::OpenOutbox {
        path: path.display().to_string(),
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique journal evidence path",
        ),
    })
}
