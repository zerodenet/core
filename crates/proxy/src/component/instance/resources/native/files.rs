//! Blocking jobs stay in this actual grant owner when their caller goes away.
mod read;
mod shutdown;
use super::*;
use std::{collections::HashMap, error::Error, fmt, io, path::Path};
use tokio::task::{Id, JoinError, JoinSet};
use zero_platform_tokio::contract::{lifecycle::CloseReport, Stage};

type SharedResult = Result<Arc<[u8]>, Arc<io::Error>>;
enum Cached {
    Unused,
    Running,
    Done(SharedResult),
}
struct FileGrant {
    max_bytes: usize,
    cached: Cached,
}
#[derive(Default)]
pub(super) struct FileReads {
    grants: BTreeMap<PathBuf, FileGrant>,
    jobs: JoinSet<io::Result<Vec<u8>>>,
    pub(super) records: HashMap<Id, PathBuf>,
    pub(super) receipts: CloseReport,
    pub(super) failure: Option<Arc<ChannelError>>,
    #[cfg(test)]
    before_read: Option<Arc<dyn Fn() + Send + Sync>>,
}
impl FileReads {
    pub(super) fn declare(
        &mut self,
        component: ComponentId,
        path: &Path,
        max_bytes: usize,
    ) -> Result<(), ChannelError> {
        if path.as_os_str().is_empty()
            || max_bytes.checked_add(1).is_none()
            || self.grants.contains_key(path)
        {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "empty, overflowing or duplicate file declaration",
            ));
        }
        self.grants.insert(
            path.to_path_buf(),
            FileGrant {
                max_bytes,
                cached: Cached::Unused,
            },
        );
        Ok(())
    }
    fn register(
        &mut self,
        component: ComponentId,
        path: &Path,
        requested: usize,
    ) -> Result<(), ChannelError> {
        let grant = self
            .grants
            .get_mut(path)
            .ok_or_else(|| error(component, Reason::InvalidConfig, "undeclared file access"))?;
        if requested > grant.max_bytes {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "requested file bound exceeds its declaration",
            ));
        }
        if matches!(grant.cached, Cached::Unused) {
            let actual_path = path.to_path_buf();
            let max_bytes = requested;
            #[cfg(test)]
            let before = self.before_read.clone();
            // Admission and registration share the same enclosing state lock as
            // shutdown. This job is owned before any waiting future can suspend.
            let handle = self.jobs.spawn_blocking(move || {
                #[cfg(test)]
                if let Some(before) = before {
                    before();
                }
                zero_platform_tokio::file::read_regular_file(&actual_path, max_bytes)
            });
            self.records.insert(handle.id(), path.to_path_buf());
            grant.cached = Cached::Running;
        }
        Ok(())
    }
    fn record(
        &mut self,
        component: ComponentId,
        result: Result<(Id, io::Result<Vec<u8>>), JoinError>,
    ) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(source) => source.id(),
        };
        let path = self.records.remove(&id).expect("actual owned file task");
        let result = match result {
            Ok((_, result)) => result.map(Arc::from).map_err(Arc::new),
            Err(source) => {
                let source = Arc::new(io::Error::other(source));
                self.failure.get_or_insert_with(|| {
                    Arc::new(file_failure(component, Stage::Close, source.clone()))
                });
                Err(source)
            }
        };
        self.grants
            .get_mut(&path)
            .expect("declared owned file task")
            .cached = Cached::Done(result);
        // This is an actual blocking-task join receipt, not a native descriptor
        // close report, a protocol task ID, or transferred user-I/O confirmation.
        self.receipts.released_resources.push(format!(
            "component {} file read task {id} joined",
            component.0
        ));
    }
}
#[derive(Debug)]
struct FileFailure(Arc<io::Error>);
impl fmt::Display for FileFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for FileFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // io::Error forwards source() to its payload's source(), skipping the
        // payload itself. Expose the actual JoinError so callers can inspect
        // its panic/cancellation details; native OS errors remain io::Error.
        if let Some(source) = self
            .0
            .get_ref()
            .and_then(|error| error.downcast_ref::<JoinError>())
        {
            Some(source)
        } else {
            Some(self.0.as_ref())
        }
    }
}
fn file_failure(component: ComponentId, stage: Stage, source: Arc<io::Error>) -> ChannelError {
    let reason = if source
        .get_ref()
        .is_some_and(|error| error.is::<JoinError>())
    {
        Reason::Internal
    } else {
        match source.kind() {
            io::ErrorKind::InvalidData => Reason::Capacity,
            io::ErrorKind::InvalidInput => Reason::InvalidConfig,
            io::ErrorKind::Unsupported => Reason::Unsupported,
            _ => Reason::Io,
        }
    };
    let mut failure =
        error(component, reason, "declared file operation failed").with_source(FileFailure(source));
    failure.stage = stage;
    failure
}

#[cfg(all(test, unix))]
#[path = "../../../tests/resource_files.rs"]
mod tests;
