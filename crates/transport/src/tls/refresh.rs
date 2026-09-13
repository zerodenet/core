//! Certificate/OCSP file refresh owned by the TLS resolver, never the proxy adapter.
use super::server::{self, Certificate};
use std::{
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
#[derive(Debug)]
struct State {
    values: RwLock<Arc<Vec<Certificate>>>,
    files: Vec<zero_traits::TlsCertificateFiles>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

#[derive(Debug)]
struct AuthorityState {
    values: RwLock<Arc<Vec<Arc<super::authority::Authority>>>>,
    files: Vec<zero_traits::TlsCertificateFiles>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

#[derive(Debug)]
pub(super) struct Authorities {
    state: Arc<AuthorityState>,
    interval: Duration,
    enabled: bool,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Authorities {
    pub(super) fn new(
        values: Vec<Arc<super::authority::Authority>>,
        files: Vec<zero_traits::TlsCertificateFiles>,
        provider: Arc<rustls::crypto::CryptoProvider>,
        options: &zero_traits::ServerTlsOptions,
    ) -> Self {
        let result = Self {
            state: Arc::new(AuthorityState {
                values: RwLock::new(Arc::new(values)),
                files,
                provider,
            }),
            interval: Duration::from_secs(if options.reload_interval_secs == 0 {
                3600
            } else {
                options.reload_interval_secs
            }),
            enabled: !options.one_time_loading,
            worker: Mutex::new(None),
        };
        result.start();
        result
    }

    fn start(&self) {
        if !self.enabled || self.state.files.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        *worker = Some(runtime.spawn(run_authorities(Arc::downgrade(&self.state), self.interval)));
    }

    pub(super) fn current(&self) -> Arc<Vec<Arc<super::authority::Authority>>> {
        self.start();
        self.state
            .values
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl Drop for Authorities {
    fn drop(&mut self) {
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            worker.abort();
        }
    }
}

async fn run_authorities(state: std::sync::Weak<AuthorityState>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(state) = state.upgrade() else {
            return;
        };
        for index in 0..state.files.len() {
            let files = state.files[index].clone();
            let provider = state.provider.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                super::authority::Authority::load(&files, &provider).map(Arc::new)
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
#[derive(Debug)]
pub(super) struct Certificates {
    state: Arc<State>,
    intervals: Vec<Duration>,
    enabled: bool,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Certificates {
    pub fn new(
        values: Vec<Certificate>,
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
        let result = Self {
            state: Arc::new(State {
                values: RwLock::new(Arc::new(values)),
                files,
                provider,
            }),
            intervals,
            enabled: !options.one_time_loading,
            worker: Mutex::new(None),
        };
        result.start();
        result
    }
    fn start(&self) {
        if !self.enabled || self.intervals.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut worker = self.worker.lock().unwrap_or_else(|e| e.into_inner());
        if worker.as_ref().is_some_and(|worker| !worker.is_finished()) {
            return;
        }
        let state = Arc::downgrade(&self.state);
        let intervals = self.intervals.clone();
        let now = tokio::time::Instant::now();
        let deadlines: Vec<_> = self
            .state
            .files
            .iter()
            .zip(&intervals)
            .map(|(files, interval)| {
                if files.ocsp_stapling_secs != 0 && files.ocsp_path.is_none() {
                    now
                } else {
                    now + *interval
                }
            })
            .collect();
        *worker = Some(runtime.spawn(run(state, intervals, deadlines)));
    }

    pub fn current(&self) -> Arc<Vec<Certificate>> {
        self.start();
        self.state
            .values
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
impl Drop for Certificates {
    fn drop(&mut self) {
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            worker.abort();
        }
    }
}

async fn run(
    state: std::sync::Weak<State>,
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
            let files = state.files[index].clone();
            let provider = state.provider.clone();
            let loaded = tokio::task::spawn_blocking(move || server::load(&files, &provider)).await;
            let files = &state.files[index];
            if let Ok(Ok(mut value)) = loaded {
                // Never attach the old leaf's staple to a replacement certificate.
                if files.ocsp_path.is_none() && value.key.cert == previous.key.cert {
                    Arc::make_mut(&mut value.key).ocsp = previous.key.ocsp.clone();
                }
                if files.ocsp_stapling_secs != 0 && files.ocsp_path.is_none() {
                    match &client {
                        Ok(client) => match super::ocsp::retrieve(client, &value.key.cert).await {
                            Ok(staple) => Arc::make_mut(&mut value.key).ocsp = Some(staple),
                            Err(error) => {
                                tracing::warn!(%error, "TLS OCSP refresh retained the previous staple for the same certificate")
                            }
                        },
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
