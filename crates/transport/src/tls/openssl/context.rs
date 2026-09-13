use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use openssl::ssl::{Ssl, SslContext};
use zero_traits::{ServerTlsProfile, TlsBackend};

use crate::profile::OwnedServerTlsProfile;

#[derive(Clone)]
pub enum TlsAcceptor {
    Rustls(tokio_rustls::TlsAcceptor),
    OpenSsl(OpenSslServerContext),
}

impl From<Arc<rustls::ServerConfig>> for TlsAcceptor {
    fn from(config: Arc<rustls::ServerConfig>) -> Self {
        Self::Rustls(tokio_rustls::TlsAcceptor::from(config))
    }
}

#[derive(Clone)]
pub struct OpenSslServerContext {
    runtime: Arc<Runtime>,
}

struct Runtime {
    current: RwLock<Arc<PreparedContext>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    ocsp_worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    updated: Arc<tokio::sync::Notify>,
    source: ContextSource,
    interval: Duration,
}

#[derive(Clone)]
struct ContextSource {
    profile: OwnedServerTlsProfile,
    base_dir: Option<PathBuf>,
}

pub(super) struct PreparedContext {
    pub(super) context: SslContext,
    pub(super) ocsp_targets: Vec<super::ocsp::OcspTarget>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            worker.abort();
        }
        if let Some(worker) = self
            .ocsp_worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            worker.abort();
        }
    }
}

impl OpenSslServerContext {
    pub(crate) fn build(
        profile: &(impl ServerTlsProfile + ?Sized),
        base_dir: Option<&Path>,
    ) -> io::Result<Self> {
        require_openssl_4_0_2()?;
        let source = ContextSource {
            profile: OwnedServerTlsProfile::from_profile(profile),
            base_dir: base_dir.map(Path::to_path_buf),
        };
        let prepared = Arc::new(super::server::build_prepared(
            &source.profile,
            source.base_dir.as_deref(),
        )?);
        let options = source.profile.tls_options();
        let interval = Duration::from_secs(if options.reload_interval_secs == 0 {
            3600
        } else {
            options.reload_interval_secs
        });
        let runtime = Arc::new(Runtime {
            current: RwLock::new(prepared),
            worker: Mutex::new(None),
            ocsp_worker: Mutex::new(None),
            updated: Arc::new(tokio::sync::Notify::new()),
            source,
            interval,
        });
        let result = Self { runtime };
        if !options.one_time_loading {
            result.start_refresh();
            result.start_ocsp();
        }
        Ok(result)
    }

    pub(crate) fn new_server_ssl(&self) -> io::Result<Ssl> {
        let current = self
            .runtime
            .current
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        Ssl::new(&current.context).map_err(io::Error::other)
    }

    fn start_refresh(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .runtime
            .worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let runtime = Arc::downgrade(&self.runtime);
        *worker = Some(handle.spawn(async move {
            loop {
                let Some(current) = runtime.upgrade() else {
                    return;
                };
                let interval = current.interval;
                drop(current);
                tokio::time::sleep(interval).await;
                let Some(current) = runtime.upgrade() else {
                    return;
                };
                let source = current.source.clone();
                drop(current);
                let next = tokio::task::spawn_blocking(move || super::server::build_prepared(&source.profile, source.base_dir.as_deref())).await;
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
                        super::ocsp::inherit(&next.ocsp_targets, &previous.ocsp_targets);
                        *runtime
                            .current
                            .write()
                            .unwrap_or_else(|error| error.into_inner()) = Arc::new(next);
                        runtime.updated.notify_waiters();
                    }
                    Ok(Err(error)) => tracing::warn!(%error, "OpenSSL TLS refresh retained the last known good context"),
                    Err(error) => tracing::warn!(%error, "OpenSSL TLS refresh task failed and retained the last known good context"),
                }
            }
        }));
    }

    fn start_ocsp(&self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self
            .runtime
            .ocsp_worker
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        *worker = Some(handle.spawn(run_ocsp(Arc::downgrade(&self.runtime))));
    }
}

async fn run_ocsp(runtime: std::sync::Weak<Runtime>) {
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
        if observed
            .as_ref()
            .is_none_or(|previous| !Arc::ptr_eq(previous, &current))
        {
            observed = Some(current.clone());
            deadlines = vec![tokio::time::Instant::now(); current.ocsp_targets.len()];
        }
        let updated = state.updated.clone();
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

pub(crate) fn use_openssl_server(profile: &(impl ServerTlsProfile + ?Sized)) -> io::Result<bool> {
    let options = profile.tls_options();
    let required = ztls::settings::needs_openssl_parameters(&options.parameters)
        .map_err(io::Error::other)?
        || !options.ech_server_keys.is_empty();
    match options.backend {
        TlsBackend::Auto => Ok(required),
        TlsBackend::Rustls => Ok(false),
        TlsBackend::OpenSsl => Ok(true),
    }
}

fn require_openssl_4_0_2() -> io::Result<()> {
    let version = openssl::version::version();
    if version.starts_with("OpenSSL 4.0.2") {
        Ok(())
    } else {
        Err(invalid(format!(
            "OpenSSL TLS backend requires vendored OpenSSL 4.0.2, loaded {version}"
        )))
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
#[path = "../../../tests/tls/openssl.rs"]
mod tests;
