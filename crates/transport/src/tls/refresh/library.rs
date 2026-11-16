//! Explicit standalone resolver convenience, never installed by a standard Source.
use super::super::server;
use super::*;
use std::{
    sync::{Mutex, Weak},
    time::Duration,
};
#[derive(Debug, Default)]
struct Worker(Mutex<Option<tokio::task::JoinHandle<()>>>);
impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(job) = self.0.get_mut().unwrap_or_else(|e| e.into_inner()).take() {
            job.abort();
        }
    }
}
#[derive(Debug)]
pub(super) struct Certificates {
    state: Weak<State>,
    files: Vec<zero_traits::TlsCertificateFiles>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    intervals: Vec<Duration>,
    worker: Worker,
}
impl Certificates {
    pub(super) fn new(
        state: Weak<State>,
        files: Vec<zero_traits::TlsCertificateFiles>,
        provider: Arc<rustls::crypto::CryptoProvider>,
        options: &zero_traits::ServerTlsOptions,
    ) -> Self {
        let intervals = files
            .iter()
            .map(|files| {
                Duration::from_secs(if files.ocsp_stapling_secs != 0 {
                    files.ocsp_stapling_secs
                } else if options.reload_interval_secs != 0 {
                    options.reload_interval_secs
                } else {
                    3600
                })
            })
            .collect();
        Self {
            state,
            files,
            provider,
            intervals,
            worker: Worker::default(),
        }
    }
    pub(super) fn start(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self.worker.0.lock().unwrap_or_else(|e| e.into_inner());
        if worker.as_ref().is_some_and(|job| !job.is_finished()) {
            return;
        }
        let now = tokio::time::Instant::now();
        let deadlines = self
            .files
            .iter()
            .zip(&self.intervals)
            .map(|(files, interval)| {
                if files.ocsp_stapling_secs != 0 && files.ocsp_path.is_none() {
                    now
                } else {
                    now + *interval
                }
            })
            .collect();
        *worker = Some(runtime.spawn(run(
            self.state.clone(),
            self.files.clone(),
            self.provider.clone(),
            self.intervals.clone(),
            deadlines,
        )));
    }
}
#[derive(Debug)]
pub(super) struct Authorities {
    state: Weak<AuthorityState>,
    files: Vec<zero_traits::TlsCertificateFiles>,
    provider: Arc<rustls::crypto::CryptoProvider>,
    interval: Duration,
    worker: Worker,
}
impl Authorities {
    pub(super) fn new(
        state: Weak<AuthorityState>,
        files: Vec<zero_traits::TlsCertificateFiles>,
        provider: Arc<rustls::crypto::CryptoProvider>,
        options: &zero_traits::ServerTlsOptions,
    ) -> Self {
        Self {
            state,
            files,
            provider,
            interval: Duration::from_secs(if options.reload_interval_secs == 0 {
                3600
            } else {
                options.reload_interval_secs
            }),
            worker: Worker::default(),
        }
    }
    pub(super) fn start(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self.worker.0.lock().unwrap_or_else(|e| e.into_inner());
        if worker.as_ref().is_some_and(|job| !job.is_finished()) {
            return;
        }
        *worker = Some(runtime.spawn(run_authorities(
            self.state.clone(),
            self.files.clone(),
            self.provider.clone(),
            self.interval,
        )));
    }
}
async fn run_authorities(
    state: std::sync::Weak<AuthorityState>,
    source_files: Vec<zero_traits::TlsCertificateFiles>,
    source_provider: Arc<rustls::crypto::CryptoProvider>,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(state) = state.upgrade() else {
            return;
        };
        for index in 0..source_files.len() {
            let files = source_files[index].clone();
            let provider = source_provider.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                super::super::authority::Authority::load(&files, &provider).map(Arc::new)
            })
            .await;
            if let Ok(Ok(value)) = loaded {
                let mut values = state.values.write().unwrap_or_else(|e| e.into_inner());
                Arc::make_mut(&mut values)[index] = value;
            } else {
                tracing::warn!("TLS authority refresh retained the previous certificate and key");
            }
        }
    }
}
async fn run(
    state: std::sync::Weak<State>,
    source_files: Vec<zero_traits::TlsCertificateFiles>,
    source_provider: Arc<rustls::crypto::CryptoProvider>,
    intervals: Vec<Duration>,
    mut deadlines: Vec<tokio::time::Instant>,
) {
    let client = crate::http_client::HttpClient::new();
    loop {
        tokio::time::sleep_until(*deadlines.iter().min().unwrap()).await;
        let Some(state) = state.upgrade() else {
            return;
        };
        for index in 0..deadlines.len() {
            if deadlines[index] > tokio::time::Instant::now() {
                continue;
            }
            let previous = state.values.read().unwrap_or_else(|e| e.into_inner())[index].clone();
            let files = source_files[index].clone();
            let provider = source_provider.clone();
            let loaded = tokio::task::spawn_blocking(move || server::load(&files, &provider)).await;
            let files = &source_files[index];
            if let Ok(Ok(mut value)) = loaded {
                // Never attach the old leaf's staple to a replacement certificate.
                if files.ocsp_path.is_none() && value.key.cert == previous.key.cert {
                    Arc::make_mut(&mut value.key).ocsp = previous.key.ocsp.clone();
                }
                if files.ocsp_stapling_secs != 0 && files.ocsp_path.is_none() {
                    match &client {
                        Ok(client) => {
                            match super::super::ocsp::retrieve(client, &value.key.cert).await {
                                Ok(staple) => Arc::make_mut(&mut value.key).ocsp = Some(staple),
                                Err(error) => {
                                    tracing::warn!(%error, "TLS OCSP refresh retained the previous staple for the same certificate")
                                }
                            }
                        }
                        Err(error) => {
                            tracing::warn!(%error, "TLS OCSP HTTP client could not be prepared")
                        }
                    }
                }
                let mut values = state.values.write().unwrap_or_else(|e| e.into_inner());
                Arc::make_mut(&mut values)[index] = value;
            } else {
                tracing::warn!("TLS certificate refresh retained the previous certificate");
            }
            deadlines[index] = tokio::time::Instant::now() + intervals[index];
        }
    }
}
