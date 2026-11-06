//! TLS material belongs to the same original protocol owner and File provider.
use super::Options;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{ResourceAccess, ResourceDeclaration},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
use zero_transport::{
    profile::OwnedClientTlsProfile,
    tls::{OwnedClientRecipe, OwnedPreparedClient},
};
mod registry;
use registry::Census;
pub(crate) use registry::Registry;
pub(crate) struct Material {
    file: Option<OwnedClientRecipe>,
    profile: OwnedClientTlsProfile,
    server: String,
    prepared: Mutex<Option<OwnedPreparedClient>>,
    census: Arc<Census>,
    stamp: std::sync::atomic::AtomicU64,
    identity: Arc<[u8]>,
}
impl Options {
    /// Keep the existing full profile and registry. This allocates neither an
    /// original fd nor tasks, and cannot inherit bytes from path equality.
    pub fn with_owned_material(
        mut self,
        runtime: &crate::transport::VlessTransportRuntime,
        source_dir: Option<&Path>,
        max_bytes: std::num::NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        let Some(profile) = &self.tls else {
            return Ok(self);
        };
        let file = profile
            .ca_cert_path
            .as_deref()
            .map(|path| {
                let path = Path::new(path);
                let path = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    source_dir.unwrap_or(Path::new(".")).join(path)
                };
                OwnedClientRecipe::new(path, source_dir, max_bytes.get())
            })
            .transpose()?;
        let mut key = self.identity.to_vec();
        key.extend_from_slice(b"vless.original-client-material.v1");
        if let Some(file) = &file {
            file.append_identity(&mut key);
        }
        let census = runtime.client_material.access(&key);
        static STAMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let stamp = if file.is_some() {
            STAMP
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |current| current.checked_add(1),
                )
                .map_err(|_| std::io::Error::other("actual TLS recipe equality exhausted"))?
        } else {
            0
        };
        let identity: Arc<[u8]> = key.clone().into();
        key.extend_from_slice(&stamp.to_le_bytes());
        self.material = Some(Arc::new(Material {
            file,
            profile: profile.clone(),
            server: self.server.clone(),
            prepared: Mutex::new(None),
            census,
            stamp: std::sync::atomic::AtomicU64::new(stamp),
            identity,
        }));
        self.identity = key.into();
        Ok(self)
    }
    /// The host separately proves same Source/config and supplies this exact
    /// full recipe's previous actual component instance. No IO or close credit.
    pub fn reuse_file_material_from(
        &mut self,
        previous: ComponentInstanceId,
    ) -> Result<bool, zero_transport::RuntimeError> {
        let Some(material) = &self.material else {
            return Ok(false);
        };
        let inherited = material.reuse(previous)?;
        if inherited {
            let mut identity = material.identity.to_vec();
            identity.extend_from_slice(
                &material
                    .stamp
                    .load(std::sync::atomic::Ordering::Acquire)
                    .to_le_bytes(),
            );
            self.identity = identity.into();
        }
        Ok(inherited)
    }
}
impl Material {
    pub(super) fn resource(&self) -> Option<ResourceDeclaration> {
        self.file.as_ref().map(|file| ResourceDeclaration::File {
            path: file.path().to_path_buf(),
            max_bytes: file.max_bytes(),
        })
    }
    pub(super) async fn start(
        self: &Arc<Self>,
        resources: &dyn ResourceAccess,
        instance: ComponentInstanceId,
        cancel: &CancellationToken,
        end: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        let prepared = match &self.file {
            Some(file) => {
                file.prepare(
                    &self.profile,
                    &self.server,
                    resources,
                    instance,
                    cancel,
                    end,
                )
                .await?
            }
            None => OwnedPreparedClient::from_material(
                &OwnedClientRecipe::validation_profile(&self.profile)
                    .map_err(|source| error(instance).with_source(source))?,
                None,
                &self.server,
                None,
            )
            .map_err(|source| error(instance).with_source(source))?,
        };
        *self.prepared.lock().unwrap() = Some(prepared);
        Ok(())
    }
    pub(super) fn record(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
    ) -> Result<(), ChannelError> {
        self.census.record(instance, self)
    }
    pub(super) async fn prepare(
        &self,
        resolver: Option<&dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> std::io::Result<OwnedPreparedClient> {
        let dynamic = matches!(
            self.profile
                .options
                .ech
                .source()
                .map_err(std::io::Error::other)?,
            Some(zero_traits::EchConfigSource::Dns { .. })
        );
        if !dynamic {
            return self.prepared.lock().unwrap().clone().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "same VLESS material owner is not started",
                )
            });
        }
        let resolver = resolver.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "actual Node ECH resolver is absent",
            )
        })?;
        let mut profile = self.profile.clone();
        zero_transport::tls::ech::prepare_options(
            &mut profile.options,
            profile.server_name.as_deref(),
            &self.server,
            resolver,
        )
        .await?;
        match &self.file {
            Some(file) => file.prepare_material(&profile, &self.server),
            None => OwnedPreparedClient::from_material(&profile, None, &self.server, None),
        }
    }
    fn reuse(&self, previous: ComponentInstanceId) -> std::io::Result<bool> {
        let Some(file) = &self.file else {
            return Ok(false);
        };
        let Some(previous) = self.census.previous(previous) else {
            return Ok(false);
        };
        let inherited = match &previous.file {
            Some(previous) => file.reuse_material_from(previous)?,
            None => false,
        };
        if inherited {
            self.stamp.store(
                previous.stamp.load(std::sync::atomic::Ordering::Acquire),
                std::sync::atomic::Ordering::Release,
            );
        }
        Ok(inherited)
    }
}
fn error(instance: ComponentInstanceId) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component);
    error.component = Some(instance.component);
    error
}
