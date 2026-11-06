//! Same outbox scan/index with read IO borrowed from its actual source.
use super::*;
#[derive(Debug)]
struct WorkFailure(Arc<ConnectorError>);
impl std::fmt::Display for WorkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for WorkFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn outbox_snapshot(
    path: &Path,
    attempt: &owner::Attempt,
) -> io::Result<(u64, io::Result<delivery_outbox::OutboxInspection>)> {
    attempt
        .source
        .inspect_seekable_file_snapshot(path, |bytes, file| {
            delivery_outbox::inspect_snapshot(path, file, bytes, &attempt.resources)
                .map_err(|error| io::Error::other(WorkFailure(Arc::new(error))))
        })
}
pub(super) fn inspect_outbox(
    kind: &str,
    configured_path: &str,
    source_dir: Option<&Path>,
    attempt: &owner::Attempt,
) -> ConnectorStateFile {
    let path = resolve_path(configured_path, source_dir);
    let (bytes, decoded) = match outbox_snapshot(&path, attempt) {
        Ok(snapshot) => snapshot,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return missing(kind, "zero.connector.delivery-outbox.v1", &path)
        }
        Err(error) => {
            let message = error.to_string();
            attempt.work_failed(error);
            return incompatible(kind, "zero.connector.delivery-outbox.v1", &path, 0, message);
        }
    };
    match decoded {
        Ok(inspection) => ConnectorStateFile {
            kind: kind.into(),
            format: "zero.connector.delivery-outbox.v1".into(),
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
        Err(error) => {
            let message = error.to_string();
            attempt.work_failed(error);
            incompatible(
                kind,
                "zero.connector.delivery-outbox.v1",
                &path,
                bytes,
                message,
            )
        }
    }
}

#[cfg(test)]
mod standalone;
#[cfg(test)]
pub(crate) use standalone::inspect_outbox_path;
