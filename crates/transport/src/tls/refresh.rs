//! Passive server certificate snapshots. Hosted values have no library runtime.
use super::server::Certificate;
use std::sync::{Arc, RwLock};
mod library;
#[derive(Debug)]
struct State {
    values: RwLock<Arc<Vec<Certificate>>>,
}
#[derive(Debug)]
struct AuthorityState {
    values: RwLock<Arc<Vec<Arc<super::authority::Authority>>>>,
}
#[derive(Debug, Clone)]
pub(super) struct Certificates {
    state: Arc<State>,
    library: Option<Arc<library::Certificates>>,
}
#[derive(Debug, Clone)]
pub(super) struct Authorities {
    state: Arc<AuthorityState>,
    library: Option<Arc<library::Authorities>>,
}
impl Certificates {
    pub fn new(
        values: Vec<Certificate>,
        files: Vec<zero_traits::TlsCertificateFiles>,
        provider: Arc<rustls::crypto::CryptoProvider>,
        options: &zero_traits::ServerTlsOptions,
    ) -> Self {
        let state = Arc::new(State {
            values: RwLock::new(Arc::new(values)),
        });
        let library = (!options.one_time_loading && !files.is_empty()).then(|| {
            Arc::new(library::Certificates::new(
                Arc::downgrade(&state),
                files,
                provider,
                options,
            ))
        });
        let result = Self { state, library };
        if let Some(library) = &result.library {
            library.start();
        }
        result
    }
    pub(super) fn owned() -> Self {
        Self {
            state: Arc::new(State {
                values: RwLock::new(Arc::new(Vec::new())),
            }),
            library: None,
        }
    }
    pub fn current(&self) -> Arc<Vec<Certificate>> {
        if let Some(library) = &self.library {
            library.start();
        }
        self.state
            .values
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(super) fn publish(&self, values: Vec<Certificate>) {
        *self.state.values.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(values);
    }
}
impl Authorities {
    pub(super) fn new(
        values: Vec<Arc<super::authority::Authority>>,
        files: Vec<zero_traits::TlsCertificateFiles>,
        provider: Arc<rustls::crypto::CryptoProvider>,
        options: &zero_traits::ServerTlsOptions,
    ) -> Self {
        let state = Arc::new(AuthorityState {
            values: RwLock::new(Arc::new(values)),
        });
        let library = (!options.one_time_loading && !files.is_empty()).then(|| {
            Arc::new(library::Authorities::new(
                Arc::downgrade(&state),
                files,
                provider,
                options,
            ))
        });
        let result = Self { state, library };
        if let Some(library) = &result.library {
            library.start();
        }
        result
    }
    pub(super) fn owned() -> Self {
        Self {
            state: Arc::new(AuthorityState {
                values: RwLock::new(Arc::new(Vec::new())),
            }),
            library: None,
        }
    }
    pub(super) fn current(&self) -> Arc<Vec<Arc<super::authority::Authority>>> {
        if let Some(library) = &self.library {
            library.start();
        }
        self.state
            .values
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(super) fn publish(&self, values: Vec<Arc<super::authority::Authority>>) {
        *self.state.values.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(values);
    }
}
