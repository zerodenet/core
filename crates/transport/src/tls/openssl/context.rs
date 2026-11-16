//! Cipher context snapshots. The standard Source supplies their actual lifecycle.
use crate::profile::OwnedServerTlsProfile;
use openssl::ssl::{Ssl, SslContext};
use std::{
    io,
    path::Path,
    sync::{Arc, RwLock},
};
use zero_traits::{ServerTlsProfile, TlsBackend};
mod library;
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
    state: Arc<ServerState>,
}
struct ServerState {
    current: RwLock<Option<Arc<PreparedContext>>>,
    library: Option<library::Library>,
}
pub(crate) struct PreparedContext {
    pub(super) context: SslContext,
    pub(super) ocsp_targets: Vec<super::ocsp::OcspTarget>,
}
impl OpenSslServerContext {
    pub(crate) fn build(
        profile: &(impl ServerTlsProfile + ?Sized),
        base_dir: Option<&Path>,
    ) -> io::Result<Self> {
        require_openssl_4_0_2()?;
        let profile = OwnedServerTlsProfile::from_profile(profile);
        let prepared = super::server::build_prepared(&profile, base_dir)?;
        let library =
            (!profile.options.one_time_loading).then(|| library::Library::new(profile, base_dir));
        let state = Arc::new(ServerState {
            current: RwLock::new(Some(Arc::new(prepared))),
            library,
        });
        if let Some(library) = &state.library {
            library.start(&state);
        }
        Ok(Self { state })
    }
    pub(crate) fn new_server_ssl(&self) -> io::Result<Ssl> {
        let current = self
            .state
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| io::Error::other("original hosted TLS material is not started"))?;
        Ssl::new(&current.context).map_err(io::Error::other)
    }
    pub(crate) fn owned() -> io::Result<Self> {
        require_openssl_4_0_2()?;
        Ok(Self {
            state: Arc::new(ServerState {
                current: RwLock::new(None),
                library: None,
            }),
        })
    }
    pub(crate) fn publish_material(
        &self,
        profile: &OwnedServerTlsProfile,
        materials: &[super::super::source::FileMaterial],
    ) -> io::Result<()> {
        let next = super::server::build_material(profile, materials)?;
        let mut current = self
            .state
            .current
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(previous) = current.as_ref() {
            super::ocsp::inherit(&next.ocsp_targets, &previous.ocsp_targets);
        }
        *current = Some(Arc::new(next));
        Ok(())
    }
    pub(crate) fn owned_ocsp_targets(&self) -> Vec<super::ocsp::OcspTarget> {
        self.state
            .current
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|value| value.ocsp_targets.clone())
            .unwrap_or_default()
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
