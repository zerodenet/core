//! A specific cache download over the same source's retained original-file slot.
use super::*;
use std::sync::{
    atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
    Arc, MutexGuard, OnceLock,
};
mod files;
pub const MAX_RULE_SET_CACHE_BYTES: u64 = 1024 * 1024 * 1024;
const ACTIVE: u8 = 0;
const COMMITTED: u8 = 1;
const ABORTED: u8 = 2;
#[derive(Debug)]
struct Identity {
    terminal: AtomicU8,
    views: AtomicUsize,
    bytes: AtomicU64,
    failure: OnceLock<Arc<io::Error>>,
}
pub(super) struct Attempt {
    identity: Arc<Identity>,
    _retained_source: Arc<ConfigurationSource>,
    path: PathBuf,
    temporary: Option<tempfile::TempPath>,
    backup: Option<PathBuf>,
    bytes: u64,
    frozen: bool,
    native_closed: bool,
    installed: bool,
    committed: bool,
    namespace_complete: bool,
}
pub(super) type CacheState = Mutex<Option<Attempt>>;
/// Clones reference the same finite attempt. No native File/view leaves the
/// source; finish/abort synchronously revoke chunk admission under its lock.
#[derive(Debug)]
pub struct RuleSetCacheDownload {
    source: Arc<ConfigurationSource>,
    identity: Arc<Identity>,
}
#[derive(Debug)]
struct Failure(Arc<io::Error>);
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), Failure(error.clone()))
}
fn remember(attempt: &Attempt, error: io::Error) -> io::Error {
    let original = attempt.identity.failure.get_or_init(|| Arc::new(error));
    retained(original)
}
fn lock(state: &CacheState) -> MutexGuard<'_, Option<Attempt>> {
    state.lock().unwrap_or_else(|poison| {
        let mut state = poison.into_inner();
        if let Some(attempt) = state.as_mut() {
            attempt.frozen = true;
            let _ = remember(
                attempt,
                io::Error::other("cache operation interrupted; original cleanup remains owned"),
            );
        }
        state
    })
}
pub(super) fn is_retained(state: &CacheState) -> bool {
    lock(state).is_some()
}
pub(super) fn ensure_idle(state: &CacheState) -> io::Result<()> {
    match lock(state).as_ref() {
        None => Ok(()),
        Some(attempt) => Err(attempt.identity.failure.get().map_or_else(
            || {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "actual rule-set cache attempt still owns configuration preparation",
                )
            },
            retained,
        )),
    }
}
impl Clone for RuleSetCacheDownload {
    fn clone(&self) -> Self {
        let mut count = self.identity.views.load(Ordering::Acquire);
        loop {
            let next = count
                .checked_add(1)
                .expect("actual cache control-view count exhausted");
            match self.identity.views.compare_exchange(
                count,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => count = actual,
            }
        }
        Self {
            source: self.source.clone(),
            identity: self.identity.clone(),
        }
    }
}
impl Drop for RuleSetCacheDownload {
    fn drop(&mut self) {
        if self.identity.views.fetch_sub(1, Ordering::AcqRel) == 1
            && self.identity.terminal.load(Ordering::Acquire) == ACTIVE
        {
            // Observing abandonment does not close anything. The exact source
            // still owns its File/namespace and retains itself until real abort.
            super::super::startup::retain_unknown(self.source.clone());
        }
    }
}
impl ConfigurationSource {
    pub fn abort_rule_set_cache(self: &Arc<Self>) -> io::Result<()> {
        let identity = lock(&self.cache)
            .as_ref()
            .map(|attempt| attempt.identity.clone());
        let Some(identity) = identity else {
            return self.ensure_resource_io_available();
        };
        let mut count = identity.views.load(Ordering::Acquire);
        loop {
            let next = count
                .checked_add(1)
                .ok_or_else(|| io::Error::other("cache control-view count exhausted"))?;
            match identity
                .views
                .compare_exchange(count, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => count = actual,
            }
        }
        RuleSetCacheDownload {
            source: self.clone(),
            identity,
        }
        .abort()
    }
    pub fn prepare_rule_set_cache(
        self: &Arc<Self>,
        path: impl AsRef<Path>,
    ) -> io::Result<RuleSetCacheDownload> {
        self.ensure_available().map_err(io::Error::from)?;
        super::super::startup::with_cache_source(self.clone(), || {
            let path = path.as_ref().to_owned();
            std::fs::create_dir_all(super::files::parent(&path))?;
            let identity = Arc::new(Identity {
                terminal: AtomicU8::new(ACTIVE),
                views: AtomicUsize::new(0),
                bytes: AtomicU64::new(0),
                failure: OnceLock::new(),
            });
            let transaction = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if transaction.pending.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "configuration source transaction is still owned",
                ));
            }
            let mut slot = lock(&self.cache);
            if slot.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "rule-set cache attempt is still owned",
                ));
            }
            self.io.ensure_available()?;
            *slot = Some(Attempt {
                identity: identity.clone(),
                _retained_source: self.clone(),
                path,
                temporary: None,
                backup: None,
                bytes: 0,
                frozen: false,
                native_closed: false,
                installed: false,
                committed: false,
                namespace_complete: false,
            });
            let attempt = slot.as_mut().unwrap();
            let opened = self.io.start_cache_file(|| {
                let mut builder = tempfile::Builder::new();
                builder.prefix(".rule-set-download-");
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    builder.permissions(std::fs::Permissions::from_mode(0o666));
                }
                let (file, path) = builder
                    .tempfile_in(super::files::parent(&attempt.path))?
                    .into_parts();
                attempt.temporary = Some(path);
                Ok(file)
            });
            if let Err(error) = opened {
                if self.io.ensure_available().is_ok() && attempt.temporary.is_none() {
                    *slot = None;
                }
                return Err(error);
            }
            identity.views.store(1, Ordering::Release);
            Ok(RuleSetCacheDownload {
                source: self.clone(),
                identity,
            })
        })
    }
}
mod download;
#[cfg(test)]
#[path = "../../tests/configuration_cache.rs"]
mod tests;
