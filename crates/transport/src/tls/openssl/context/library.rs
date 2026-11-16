//! Explicit standalone OpenSSL refresh/OCSP convenience. Hosted contexts have None.
use super::*;
use std::{path::PathBuf, sync::Mutex, time::Duration};
#[derive(Clone)]
struct ContextSource {
    profile: OwnedServerTlsProfile,
    base_dir: Option<PathBuf>,
}
pub(super) struct Library {
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    ocsp_worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    updated: Arc<tokio::sync::Notify>,
    source: ContextSource,
    interval: Duration,
}
impl Drop for Library {
    fn drop(&mut self) {
        for slot in [&mut self.worker, &mut self.ocsp_worker] {
            if let Some(job) = slot.get_mut().unwrap_or_else(|e| e.into_inner()).take() {
                job.abort();
            }
        }
    }
}
impl Library {
    pub(super) fn new(profile: OwnedServerTlsProfile, base_dir: Option<&Path>) -> Self {
        let interval = Duration::from_secs(if profile.options.reload_interval_secs == 0 {
            3600
        } else {
            profile.options.reload_interval_secs
        });
        Self {
            worker: Mutex::new(None),
            ocsp_worker: Mutex::new(None),
            updated: Arc::new(tokio::sync::Notify::new()),
            source: ContextSource {
                profile,
                base_dir: base_dir.map(Path::to_path_buf),
            },
            interval,
        }
    }
    pub(super) fn start(&self, state: &Arc<ServerState>) {
        self.start_refresh(state);
        self.start_ocsp(state);
    }
    fn start_refresh(&self, state: &Arc<ServerState>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let runtime = Arc::downgrade(state);
        *worker = Some(handle.spawn(async move {
            loop {
                let Some(current) = runtime.upgrade() else {
                    return;
                };
                let interval = current.library.as_ref().expect("standalone library state").interval;
                drop(current);
                tokio::time::sleep(interval).await;
                let Some(current) = runtime.upgrade() else {
                    return;
                };
                let source = current.library.as_ref().expect("standalone library state").source.clone();
                drop(current);
                let next = tokio::task::spawn_blocking(move || super::super::server::build_prepared(&source.profile, source.base_dir.as_deref())).await;
                let Some(runtime) = runtime.upgrade() else {
                    return;
                };
                match next {
                    Ok(Ok(next)) => {
                        let previous = runtime
                            .current
                            .read()
                            .unwrap_or_else(|error| error.into_inner())
                            .clone();
                        if let Some(previous)=previous {super::super::ocsp::inherit(&next.ocsp_targets, &previous.ocsp_targets);}
                        *runtime
                            .current
                            .write()
                            .unwrap_or_else(|error| error.into_inner()) = Some(Arc::new(next));
                        runtime.library.as_ref().expect("standalone library state").updated.notify_waiters();
                    }
                    Ok(Err(error)) => tracing::warn!(%error, "OpenSSL TLS refresh retained the last known good context"),
                    Err(error) => tracing::warn!(%error, "OpenSSL TLS refresh task failed and retained the last known good context"),
                }
            }
        }));
    }

    fn start_ocsp(&self, state: &Arc<ServerState>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .ocsp_worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        *worker = Some(handle.spawn(run_ocsp(Arc::downgrade(state))));
    }
}
async fn run_ocsp(runtime: std::sync::Weak<ServerState>) {
    let client = crate::http_client::HttpClient::new();
    let mut observed: Option<Arc<PreparedContext>> = None;
    let mut deadlines = Vec::new();
    loop {
        let Some(state) = runtime.upgrade() else {
            return;
        };
        let current = state
            .current
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(current) = current else {
            return;
        };
        if observed
            .as_ref()
            .is_none_or(|previous| !Arc::ptr_eq(previous, &current))
        {
            observed = Some(current.clone());
            deadlines = vec![tokio::time::Instant::now(); current.ocsp_targets.len()];
        }
        let updated = state
            .library
            .as_ref()
            .expect("standalone library state")
            .updated
            .clone();
        drop(state);
        let Some(deadline) = deadlines.iter().min().copied() else {
            updated.notified().await;
            continue;
        };
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {}
            _ = updated.notified() => continue,
        }
        let now = tokio::time::Instant::now();
        for (index, target) in current.ocsp_targets.iter().enumerate() {
            if deadlines[index] > now {
                continue;
            }
            match &client {
                Ok(client) => target.refresh(client).await,
                Err(error) => {
                    tracing::warn!(%error, "OpenSSL TLS OCSP HTTP client could not be prepared")
                }
            }
            deadlines[index] = tokio::time::Instant::now() + target.interval;
        }
    }
}
