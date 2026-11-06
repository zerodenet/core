use std::{io, sync::Arc};
use zero_runtime::configuration::ConfigurationSource;
mod jsonl;
mod outbox;
use outbox::inspect_outbox;
#[cfg(test)]
pub(crate) use outbox::inspect_outbox_path;
mod lease;
mod owner;
pub(crate) use lease::PersistentStateLease;
use std::path::{Path, PathBuf};

use serde::Serialize;
use zero_config::RuntimeConfig;

use crate::dispatcher::outbox as delivery_outbox;
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

impl ConnectorStateReport {
    pub fn is_compatible(&self) -> bool {
        self.compatible
    }
}

/// The report shape stays unchanged; actual typed failures remain a separate
/// local receiver rather than being reduced to a JSON diagnostic string.
pub struct ConnectorStateInspection {
    report: ConnectorStateReport,
    failures: Vec<Arc<io::Error>>,
}
impl ConnectorStateInspection {
    pub fn into_parts(self) -> (ConnectorStateReport, Vec<Arc<io::Error>>) {
        (self.report, self.failures)
    }
}
/// Explicit library inspection retains its own genuine source. The CLI passes
/// the source which actually loaded its configuration through the next entry.
pub fn inspect_persistent_state(config: &RuntimeConfig) -> ConnectorStateReport {
    inspect_persistent_state_with_source(config, &Arc::new(ConfigurationSource::default())).report
}
pub fn inspect_persistent_state_with_source(
    config: &RuntimeConfig,
    source: &Arc<ConfigurationSource>,
) -> ConnectorStateInspection {
    let (lease, mut inherited) = match owner::begin(source.clone()) {
        Ok(lease) => lease,
        Err(failures) => {
            let message = failures
                .first()
                .map(ToString::to_string)
                .unwrap_or_else(|| "original inspection cleanup remains unconfirmed".into());
            let mut files = Vec::new();
            if let Some(path) = config.api.outbox_path.as_deref() {
                files.push(incompatible(
                    "event_outbox",
                    "zero.connector.delivery-outbox.v1",
                    &resolve_path(path, config.source_dir()),
                    0,
                    message.clone(),
                ));
            }
            if let Some(path) = config.api.dead_letter_path.as_deref() {
                files.push(incompatible(
                    "dead_letter",
                    "zero.connector.dead-letter-jsonl.v1",
                    &resolve_path(path, config.source_dir()),
                    0,
                    message,
                ));
            }
            return ConnectorStateInspection {
                report: ConnectorStateReport {
                    schema_id: CONNECTOR_STATE_REPORT_SCHEMA,
                    compatible: false,
                    files,
                },
                failures,
            };
        }
    };
    let original = lease.original.clone();
    let mut files = Vec::new();
    if let Some(path) = config.api.outbox_path.as_deref() {
        files.push(inspect_outbox(
            "event_outbox",
            path,
            config.source_dir(),
            &original,
        ));
    }
    if let Some(path) = config.api.dead_letter_path.as_deref() {
        files.push(inspect_json_lines(
            "dead_letter",
            "zero.connector.dead-letter-jsonl.v1",
            path,
            config.source_dir(),
            &original,
        ));
    }
    let (complete, faults) = lease.finish();
    if let Some(error) = original.cleanup_failure() {
        for file in &mut files {
            file.status = ConnectorStateStatus::Incompatible;
            file.error = Some(match file.error.take() {
                Some(work) => format!("{work}; original inspection cleanup failed: {error}"),
                None => format!("original inspection cleanup failed: {error}"),
            });
        }
    }
    inherited.extend(faults);
    let compatible = complete
        && files
            .iter()
            .all(|file| file.status != ConnectorStateStatus::Incompatible);
    ConnectorStateInspection {
        report: ConnectorStateReport {
            schema_id: CONNECTOR_STATE_REPORT_SCHEMA,
            compatible,
            files,
        },
        failures: inherited,
    }
}

fn inspect_json_lines(
    kind: &str,
    format: &str,
    configured_path: &str,
    source_dir: Option<&Path>,
    attempt: &owner::Attempt,
) -> ConnectorStateFile {
    let path = resolve_path(configured_path, source_dir);
    let (bytes, decoded) = match jsonl::inspect(&path, &attempt.source) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return missing(kind, format, &path)
        }
        Err(error) => {
            let message = format!("read failed: {error}");
            attempt.work_failed(error);
            return incompatible(kind, format, &path, 0, message);
        }
    };
    match decoded {
        Ok(records) => ConnectorStateFile {
            kind: kind.into(),
            format: format.into(),
            path: path.display().to_string(),
            status: ConnectorStateStatus::Ready,
            bytes,
            records: Some(records),
            pending: None,
            error: None,
        },
        Err(error) => {
            let message = error.to_string();
            attempt.work_failed(error);
            incompatible(kind, format, &path, bytes, message)
        }
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

#[cfg(test)]
mod tests;
