//! Strong immutable CA snapshots belong to the actual complete cold recipe.
use super::*;
use std::path::{Path, PathBuf};
#[cfg(feature = "tls")]
use std::sync::OnceLock;
pub(in crate::grpc::owned) struct CaFile {
    pub(in crate::grpc::owned) path: PathBuf,
    pub(in crate::grpc::owned) max_bytes: usize,
    source_dir: Option<PathBuf>,
    #[cfg(feature = "tls")]
    snapshot: OnceLock<Arc<[u8]>>,
}
impl CaFile {
    fn same(&self, path: &Path, source_dir: Option<&Path>, max_bytes: usize) -> bool {
        self.path == path && self.source_dir.as_deref() == source_dir && self.max_bytes == max_bytes
    }
    pub(super) fn append_identity(&self, identity: &mut Vec<u8>) {
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
    #[cfg(feature = "tls")]
    pub(in crate::grpc::owned) fn prepare_material(
        &self,
        profile: &crate::profile::OwnedClientTlsProfile,
        server: &str,
    ) -> std::io::Result<crate::tls::OwnedPreparedClient> {
        let bytes = self.snapshot.get().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
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
    #[cfg(feature = "tls")]
    pub(in crate::grpc::owned) async fn prepare(
        &self,
        options: &NativePoolOptions,
        resources: &dyn zero_platform_tokio::contract::component::ResourceAccess,
        instance: ComponentInstanceId,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<crate::tls::OwnedPreparedClient, ChannelError> {
        let bytes = if let Some(snapshot) = self.snapshot.get() {
            snapshot.clone()
        } else {
            let read = resources.read_file(self.path.clone(), self.max_bytes);
            let bytes = tokio::select! {biased;
                _=cancel.cancelled()=>return Err(error(instance,Stage::Prepare,Reason::Revoked)),
                result=zero_platform_tokio::contract::context::with_deadline(Some(deadline),read)=>result.map_err(|source|error(instance,Stage::Prepare,Reason::Timeout).with_source(source))??,
            };
            if bytes.len() > self.max_bytes {
                return Err(error(instance, Stage::Prepare, Reason::Capacity));
            }
            Arc::<[u8]>::from(bytes)
        };
        let tls = options
            .settings
            .tls
            .as_ref()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::StalePlan))?;
        // Cold CA validation never emits TLS I/O. Dynamic ECH is resolved in
        // the actual preparation task, using these same authorized bytes.
        let validation = super::super::ech::validation_profile(tls).map_err(|source| {
            error(instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
        })?;
        let prepared = crate::tls::OwnedPreparedClient::from_material(
            &validation,
            self.source_dir.as_deref(),
            &options.settings.server,
            Some(&bytes),
        )
        .map_err(|source| {
            error(instance, Stage::Prepare, Reason::InvalidConfig).with_source(source)
        })?;
        // Failed/partial reads never become a usable recovery recipe. Once a
        // valid recipe starts, recovery uses its same authorized immutable CA.
        let _ = self.snapshot.set(bytes);
        Ok(prepared)
    }
}
impl NativePoolOptions {
    /// New Options owns a fresh candidate snapshot. Assembly requires a fresh
    /// File owner unless its exact same-source previous instance supplied the
    /// already authorized material. Path/profile equality alone never reuses it.
    pub fn with_file_owner(
        mut self,
        source_dir: Option<&Path>,
        max_bytes: NonZeroUsize,
    ) -> Result<Self, RuntimeError> {
        if max_bytes.get().checked_add(1).is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "CA File bound overflow",
            )
            .into());
        }
        let Some(path) = self
            .settings
            .tls
            .as_ref()
            .and_then(|tls| tls.ca_cert_path.as_deref())
        else {
            return Ok(self);
        };
        self._background_recipe.as_ref().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "fixed gRPC library has no CA File owner",
            )
        })?;
        let path = Path::new(path);
        let path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            source_dir.unwrap_or(Path::new(".")).join(path)
        };
        let ca = match self
            .ca
            .as_ref()
            .filter(|file| file.same(&path, source_dir, max_bytes.get()))
        {
            Some(current) => current.clone(),
            None => Arc::new(CaFile {
                path,
                max_bytes: max_bytes.get(),
                source_dir: source_dir.map(Path::to_path_buf),
                #[cfg(feature = "tls")]
                snapshot: OnceLock::new(),
            }),
        };
        self.ca = Some(ca);
        self.refresh_identity();
        Ok(self)
    }
}

#[cfg(feature = "tls")]
impl NativePoolOptions {
    pub(in crate::grpc::owned) fn record_file_material(
        &self,
        instance: ComponentInstanceId,
    ) -> Result<(), ChannelError> {
        let Some(ca) = &self.ca else { return Ok(()) };
        if ca.snapshot.get().is_none() {
            return Err(error(instance, Stage::Prepare, Reason::StalePlan));
        }
        let recipe = self
            ._background_recipe
            .as_ref()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::StalePlan))?;
        let mut materials = recipe.materials.lock().unwrap();
        materials.retain(|(_, file)| file.strong_count() != 0);
        if let Some((_, actual)) = materials.iter().find(|(actual, _)| *actual == instance) {
            return if actual.ptr_eq(&Arc::downgrade(ca)) {
                Ok(())
            } else {
                Err(error(instance, Stage::Prepare, Reason::StalePlan))
            };
        }
        materials.try_reserve(1).map_err(|source| {
            error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        materials.push((instance, Arc::downgrade(ca)));
        Ok(())
    }
    /// Copy immutable bytes only from this same complete pool recipe's actual
    /// installed previous instance. Host assembly separately proves same
    /// configuration identity; this grants no IO, reuse of tasks or closure.
    pub fn reuse_file_material_from(
        &self,
        previous: ComponentInstanceId,
    ) -> Result<bool, RuntimeError> {
        let Some(ca) = &self.ca else { return Ok(false) };
        let Some(recipe) = &self._background_recipe else {
            return Ok(false);
        };
        let previous = {
            let materials = recipe.materials.lock().unwrap();
            materials
                .iter()
                .find(|(instance, _)| *instance == previous)
                .and_then(|(_, file)| file.upgrade())
        };
        let Some(previous) = previous
            .filter(|previous| previous.same(&ca.path, ca.source_dir.as_deref(), ca.max_bytes))
        else {
            return Ok(false);
        };
        let Some(material) = previous.snapshot.get().cloned() else {
            return Ok(false);
        };
        if let Some(current) = ca.snapshot.get() {
            return if Arc::ptr_eq(current, &material) {
                Ok(true)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "actual CA recipe already holds different material",
                )
                .into())
            };
        }
        ca.snapshot.set(material).map_err(|_| {
            std::io::Error::other("actual CA recipe changed during same-source material handoff")
        })?;
        Ok(true)
    }
}
