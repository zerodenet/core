use super::*;
/// Existing private library/interop inspector: no Proxy or writer lease starts.
pub(crate) fn inspect_outbox_path(
    path: &Path,
) -> ConnectorResult<delivery_outbox::OutboxInspection> {
    let source = Arc::new(ConfigurationSource::default());
    let (lease, _) = owner::begin(source).map_err(|faults| ConnectorError::OpenOutbox {
        path: path.display().to_string(),
        source: retained_inspection(faults),
    })?;
    let result = outbox_snapshot(path, &lease.original)
        .and_then(|(_, decoded)| decoded)
        .map_err(|error| {
            let error = Arc::new(error);
            lease.original.work_failed_arc(error.clone());
            error
        });
    let (complete, faults) = lease.finish();
    match result {
        Ok(value) if complete && faults.is_empty() => Ok(value),
        _ => {
            let sources = faults;
            Err(ConnectorError::OpenOutbox {
                path: path.display().to_string(),
                source: retained_inspection(sources),
            })
        }
    }
}
#[derive(Debug)]
struct InspectionFailures(Vec<Arc<io::Error>>);
impl std::fmt::Display for InspectionFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for InspectionFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source.as_ref() as _)
    }
}
fn retained_inspection(faults: Vec<Arc<io::Error>>) -> io::Error {
    io::Error::other(InspectionFailures(faults))
}
