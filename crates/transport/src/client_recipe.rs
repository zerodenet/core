//! Authorized immutable CA bytes shared by existing TLS-owning recipes.
//! This is material only, never a socket, Scope, cache or native permission.
#[cfg(feature = "tls")]
use std::sync::{Arc, OnceLock};
use std::{
    io,
    path::{Path, PathBuf},
};
pub struct OwnedClientRecipe {
    path: PathBuf,
    source_dir: Option<PathBuf>,
    max_bytes: usize,
    #[cfg(feature = "tls")]
    snapshot: OnceLock<Arc<[u8]>>,
}
impl OwnedClientRecipe {
    /// `path` is the exact absolute/resolved path declared by the owning Slot.
    pub fn new(path: PathBuf, source_dir: Option<&Path>, max_bytes: usize) -> io::Result<Self> {
        if max_bytes == 0 || max_bytes.checked_add(1).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid owned CA File bound",
            ));
        }
        Ok(Self {
            path,
            source_dir: source_dir.map(Path::to_path_buf),
            max_bytes,
            #[cfg(feature = "tls")]
            snapshot: OnceLock::new(),
        })
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }
    pub fn same(&self, path: &Path, source_dir: Option<&Path>, max_bytes: usize) -> bool {
        self.path == path && self.source_dir.as_deref() == source_dir && self.max_bytes == max_bytes
    }
    pub fn append_identity(&self, identity: &mut Vec<u8>) {
        // Preserve the existing on-process equality version during extraction.
        identity.extend_from_slice(b"grpc.actual-ca-file.v1");
        let path = self.path.as_os_str().as_encoded_bytes();
        identity.extend_from_slice(&(path.len() as u64).to_le_bytes());
        identity.extend_from_slice(path);
        match &self.source_dir {
            Some(base) => {
                identity.push(1);
                let bytes = base.as_os_str().as_encoded_bytes();
                identity.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
                identity.extend_from_slice(bytes);
            }
            None => identity.push(0),
        }
        identity.extend_from_slice(&(self.max_bytes as u64).to_le_bytes());
    }
}
#[cfg(feature = "tls")]
impl OwnedClientRecipe {
    pub fn has_material(&self) -> bool {
        self.snapshot.get().is_some()
    }
    /// Cold CA validation suppresses dynamic DNS only for this temporary parse.
    /// Actual connections must use the original profile with its ECH prepared.
    pub fn validation_profile(
        tls: &crate::profile::OwnedClientTlsProfile,
    ) -> io::Result<crate::profile::OwnedClientTlsProfile> {
        let mut profile = tls.clone();
        if matches!(
            profile.options.ech.source().map_err(io::Error::other)?,
            Some(zero_traits::EchConfigSource::Dns { .. })
        ) {
            profile.options.ech.prepared_config_list = Some(Vec::new());
        }
        Ok(profile)
    }
    /// Read through the same Slot's File provider, then retain only a completely
    /// validated snapshot. This never performs DNS, TLS or socket I/O.
    pub async fn prepare(
        &self,
        profile: &crate::profile::OwnedClientTlsProfile,
        server: &str,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
        instance: zero_core::component::ComponentInstanceId,
        cancel: &zero_platform_tokio::contract::CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Result<crate::tls::OwnedPreparedClient, zero_platform_tokio::contract::ChannelError> {
        use zero_platform_tokio::contract::{context::with_deadline, Reason, Stage};
        let bytes = if let Some(snapshot) = self.snapshot.get() {
            snapshot.clone()
        } else {
            let read = resources.read_file(self.path.clone(), self.max_bytes);
            let bytes = tokio::select! {biased;
                _=cancel.cancelled()=>return Err(error(instance,Stage::Prepare,Reason::Revoked)),
                result=with_deadline(Some(deadline),read)=>result.map_err(|source|error(instance,Stage::Prepare,Reason::Timeout).with_source(source))??,
            };
            if bytes.len() > self.max_bytes {
                return Err(error(instance, Stage::Prepare, Reason::Capacity));
            }
            Arc::<[u8]>::from(bytes)
        };
        let validation = Self::validation_profile(profile).map_err(|source| {
            error(instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
        })?;
        let prepared = crate::tls::OwnedPreparedClient::from_material(
            &validation,
            self.source_dir.as_deref(),
            server,
            Some(&bytes),
        )
        .map_err(|source| {
            error(instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
        })?;
        let _ = self.snapshot.set(bytes);
        Ok(prepared)
    }
    /// Per-connection dynamic ECH already resolved by the same actual Node owner.
    /// Read only previously authorized bytes; never reopen ca_cert_path.
    pub fn prepare_material(
        &self,
        profile: &crate::profile::OwnedClientTlsProfile,
        server: &str,
    ) -> io::Result<crate::tls::OwnedPreparedClient> {
        let bytes = self.snapshot.get().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actual CA File material is absent",
            )
        })?;
        crate::tls::OwnedPreparedClient::from_material(
            profile,
            self.source_dir.as_deref(),
            server,
            Some(bytes),
        )
    }
    #[cfg(feature = "quic")]
    pub fn prepare_quic_material(
        &self,
        profile: &crate::profile::OwnedClientTlsProfile,
        alpn: &[Vec<u8>],
    ) -> io::Result<crate::quic::QuicClientConfig> {
        let bytes = self.snapshot.get().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actual CA File material is absent",
            )
        })?;
        crate::quic::client_config_with_ca_material(
            profile.insecure,
            None,
            alpn,
            None,
            Some(&self.path),
            &profile.options,
            Some(bytes),
        )
        .map_err(io::Error::other)
    }
    /// The owning factory first proves the same full recipe, actual prior
    /// instance and same Source. Path equality alone cannot select `previous`.
    pub fn reuse_material_from(&self, previous: &Self) -> io::Result<bool> {
        if !previous.same(&self.path, self.source_dir.as_deref(), self.max_bytes) {
            return Ok(false);
        }
        let Some(material) = previous.snapshot.get().cloned() else {
            return Ok(false);
        };
        if let Some(current) = self.snapshot.get() {
            return if Arc::ptr_eq(current, &material) {
                Ok(true)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "actual CA recipe already holds different material",
                ))
            };
        }
        self.snapshot.set(material).map_err(|_| {
            io::Error::other("actual CA recipe changed during same-source material handoff")
        })?;
        Ok(true)
    }
}
#[cfg(feature = "tls")]
fn error(
    instance: zero_core::component::ComponentInstanceId,
    stage: zero_platform_tokio::contract::Stage,
    reason: zero_platform_tokio::contract::Reason,
) -> zero_platform_tokio::contract::ChannelError {
    let mut error = zero_platform_tokio::contract::ChannelError::new(
        stage,
        reason,
        zero_platform_tokio::contract::Impact::Component,
    );
    error.component = Some(instance.component);
    error
}
