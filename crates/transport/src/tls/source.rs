//! Cold server TLS material and the actual Source's installed refresh owner.
use super::*;
use crate::profile::OwnedServerTlsProfile;
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
use zero_platform_tokio::contract::{
    component::*,
    lifecycle::*,
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
pub(super) mod http_client;
mod lifecycle;
mod reload;
pub(crate) struct FileMaterial {
    pub(crate) files: zero_traits::TlsCertificateFiles,
    pub(crate) cert: Vec<u8>,
    pub(crate) key: Vec<u8>,
    pub(crate) ocsp: Option<Vec<u8>>,
}
const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
enum Backend {
    Rustls {
        certificates: super::refresh::Certificates,
        authorities: super::refresh::Authorities,
        provider: Arc<rustls::crypto::CryptoProvider>,
        config: Arc<rustls::ServerConfig>,
    },
    OpenSsl(openssl::OpenSslServerContext),
}
pub struct PreparedServer {
    profile: OwnedServerTlsProfile,
    files: Vec<zero_traits::TlsCertificateFiles>,
    backend: Backend,
    bound: AtomicBool,
    base: Option<std::path::PathBuf>,
    library: Mutex<Option<TlsAcceptor>>,
}
impl std::fmt::Debug for PreparedServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedServer(original Source material)")
    }
}
impl PreparedServer {
    /// Pure recipe/config creation. No file, process, socket or task starts.
    pub fn new(
        profile: &(impl ServerTlsProfile + ?Sized),
        base: Option<&Path>,
        quic: bool,
    ) -> io::Result<Arc<Self>> {
        let profile = OwnedServerTlsProfile::from_profile(profile);
        let options = profile.tls_options();
        ztls::settings::validate_server(&options, quic).map_err(io::Error::other)?;
        let files = openssl::certificates::files(&profile, base)?;
        if files.is_empty() {
            return Err(io::Error::other(
                "TLS server has no certificate or issuing authority",
            ));
        }
        let backend = if openssl::use_openssl_server(&profile)? {
            Backend::OpenSsl(openssl::OpenSslServerContext::owned()?)
        } else {
            let provider =
                super::config::provider(&options.parameters, profile.server_fingerprint(), false)?;
            let certificates = super::refresh::Certificates::owned();
            let authorities = super::refresh::Authorities::owned();
            let config = Arc::new(server::assemble(
                &profile,
                quic,
                provider.clone(),
                certificates.clone(),
                authorities.clone(),
            )?);
            Backend::Rustls {
                certificates,
                authorities,
                provider,
                config,
            }
        };
        Ok(Arc::new(Self {
            profile,
            files,
            backend,
            bound: false.into(),
            base: base.map(Path::to_path_buf),
            library: Mutex::new(None),
        }))
    }
    /// Standalone request helpers alone retain their original library executor.
    pub fn library_acceptor(&self) -> io::Result<TlsAcceptor> {
        let mut library = self.library.lock().unwrap_or_else(|e| e.into_inner());
        if self.bound.load(Ordering::Acquire) {
            return Ok(self.acceptor());
        }
        if let Some(acceptor) = library.as_ref() {
            return Ok(acceptor.clone());
        }
        let acceptor = super::build_tls_acceptor(&self.profile, self.base.as_deref())
            .map_err(io::Error::other)?;
        *library = Some(acceptor.clone());
        Ok(acceptor)
    }
    pub fn acceptor(&self) -> TlsAcceptor {
        match &self.backend {
            Backend::Rustls { config, .. } => TlsAcceptor::from(config.clone()),
            Backend::OpenSsl(context) => TlsAcceptor::OpenSsl(context.clone()),
        }
    }
    #[cfg(feature = "quic")]
    pub fn quic_config(
        &self,
        transport: quinn::TransportConfig,
    ) -> io::Result<quinn::ServerConfig> {
        let crypto: Arc<dyn quinn_proto::crypto::ServerConfig> = match &self.backend {
            Backend::Rustls { config, .. } => Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from((**config).clone())
                    .map_err(io::Error::other)?,
            ),
            Backend::OpenSsl(context) => Arc::new(openssl::quic::OpenSslQuicServerConfig::new(
                Arc::new(context.clone()),
            )),
        };
        let mut config = quinn::ServerConfig::with_crypto(crypto);
        config.transport_config(Arc::new(transport));
        Ok(config)
    }
    fn automatic_ocsp(&self) -> bool {
        !self.profile.options.one_time_loading
            && self.files.iter().any(|files| {
                files.usage != zero_traits::TlsCertificateUsage::AuthorityIssue
                    && files.ocsp_stapling_secs != 0
                    && files.ocsp_path.is_none()
            })
    }
    pub fn task_slots(&self) -> usize {
        if self.profile.options.one_time_loading {
            0
        } else {
            1 + if self.automatic_ocsp() { 3 } else { 0 }
        }
    }
    async fn material(
        &self,
        resources: &dyn ResourceAccess,
        files: &zero_traits::TlsCertificateFiles,
    ) -> Result<FileMaterial, ChannelError> {
        let cert = resources
            .read_file(files.cert_path.clone().into(), MAX_FILE_BYTES)
            .await?;
        let key = resources
            .read_file(files.key_path.clone().into(), MAX_FILE_BYTES)
            .await?;
        let ocsp = match &files.ocsp_path {
            Some(path) => Some(resources.read_file(path.into(), MAX_FILE_BYTES).await?),
            None => None,
        };
        Ok(FileMaterial {
            files: files.clone(),
            cert,
            key,
            ocsp,
        })
    }
}
/// Extend the original Source spec; no role, native acquire or fallback factory.
pub fn declare(spec: &mut ComponentSpec, plans: &[Arc<PreparedServer>]) {
    let mut files = BTreeMap::new();
    let mut tasks = spec.task_limit.map_or(0, NonZeroUsize::get);
    let mut targets = 0;
    for plan in plans {
        tasks += plan.task_slots();
        targets += usize::from(plan.automatic_ocsp());
        for recipe in &plan.files {
            for path in [
                Some(&recipe.cert_path),
                Some(&recipe.key_path),
                recipe.ocsp_path.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                files.insert(std::path::PathBuf::from(path), MAX_FILE_BYTES);
            }
        }
    }
    for (path, max_bytes) in files {
        if !spec
            .resources
            .iter()
            .any(|r| matches!(r,ResourceDeclaration::File {path:old,..} if *old==path))
        {
            spec.resources
                .push(ResourceDeclaration::File { path, max_bytes });
        }
    }
    if targets > 0 {
        spec.resources.push(ResourceDeclaration::ComponentTargets {
            max_sockets: NonZeroUsize::new(2 * targets).unwrap(),
        });
    }
    spec.task_limit = NonZeroUsize::new(tasks);
}
pub fn wrap(
    mut instance: ComponentInstance,
    plans: Vec<Arc<PreparedServer>>,
    resources: Arc<dyn ResourceAccess>,
    tasks: TaskRegistrar,
    cancel: CancellationToken,
) -> Result<ComponentInstance, BuildError> {
    for plan in &plans {
        let library = plan.library.lock().unwrap_or_else(|e| e.into_inner());
        if library.is_some() || plan.bound.swap(true, Ordering::AcqRel) {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
    }
    if !plans.is_empty() {
        instance.lifecycle = erase_lifecycle(lifecycle::Owner {
            inner: instance.lifecycle,
            plans,
            resources,
            tasks,
            stop: cancel.child_token(),
            workers: Vec::new(),
            started: false,
        });
    }
    Ok(instance)
}

#[cfg(test)]
#[path = "../../tests/tls/source.rs"]
mod tests;
