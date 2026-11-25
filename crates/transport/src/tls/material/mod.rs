//! Shared client TLS CA file material, weak cold-identity handoff, and lifecycle owner.

mod registry;

pub use registry::{BoundClientMaterial, ClientMaterialRegistry};

use crate::{
    profile::OwnedClientTlsProfile,
    tls::{ech::EchConfigResolver, OwnedClientRecipe, OwnedPreparedClient},
};
use std::{
    io,
    num::NonZeroUsize,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    admission::SessionOwner,
    component::{ResourceAccess, ResourceDeclaration},
    lifecycle::{CloseReport, ResourceLifecycle},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};

use registry::Census;

/// Client TLS CA file material bound to its owning protocol component and `File` grant.
pub struct ClientMaterial {
    file: Option<OwnedClientRecipe>,
    profile: OwnedClientTlsProfile,
    server: String,
    prepared: Mutex<Option<OwnedPreparedClient>>,
    census: Arc<Census>,
    stamp: AtomicU64,
    identity: Arc<[u8]>,
}

impl ClientMaterial {
    /// Construct cold client TLS material without opening files or spawning tasks.
    pub fn create(
        registry: &ClientMaterialRegistry,
        profile: &OwnedClientTlsProfile,
        server: String,
        source_dir: Option<&Path>,
        max_bytes: NonZeroUsize,
        base_identity: &[u8],
        domain_tag: &[u8],
    ) -> Result<(Arc<Self>, Arc<[u8]>), crate::RuntimeError> {
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
        let mut key = base_identity.to_vec();
        key.extend_from_slice(domain_tag);
        if let Some(file) = &file {
            file.append_identity(&mut key);
        }
        let census = registry.access(&key);
        static STAMP: AtomicU64 = AtomicU64::new(1);
        let stamp = if file.is_some() {
            STAMP
                .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| io::Error::other("actual TLS recipe equality exhausted"))?
        } else {
            0
        };
        let identity: Arc<[u8]> = key.clone().into();
        key.extend_from_slice(&stamp.to_le_bytes());
        let material = Arc::new(Self {
            file,
            profile: profile.clone(),
            server,
            prepared: Mutex::new(None),
            census,
            stamp: AtomicU64::new(stamp),
            identity,
        });
        Ok((material, key.into()))
    }

    pub fn resource(&self) -> Option<ResourceDeclaration> {
        self.file.as_ref().map(|file| ResourceDeclaration::File {
            path: file.path().to_path_buf(),
            max_bytes: file.max_bytes(),
        })
    }

    pub async fn start(
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
                    .map_err(|source| component_error(instance).with_source(source))?,
                None,
                &self.server,
                None,
            )
            .map_err(|source| component_error(instance).with_source(source))?,
        };
        *self.prepared.lock().unwrap() = Some(prepared);
        Ok(())
    }

    pub fn record(self: &Arc<Self>, instance: ComponentInstanceId) -> Result<(), ChannelError> {
        self.census.record(instance, self)
    }

    pub fn record_bound(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        resolver: Option<&Arc<dyn EchConfigResolver>>,
    ) -> Result<(), ChannelError> {
        self.census.record_bound(instance, self, resolver)
    }

    pub async fn prepare(
        &self,
        resolver: Option<&dyn EchConfigResolver>,
    ) -> io::Result<OwnedPreparedClient> {
        let dynamic = matches!(
            self.profile
                .options
                .ech
                .source()
                .map_err(io::Error::other)?,
            Some(zero_traits::EchConfigSource::Dns { .. })
        );
        if !dynamic {
            return self.prepared.lock().unwrap().clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "client material owner is not started",
                )
            });
        }
        let resolver = resolver.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actual Node ECH resolver is absent",
            )
        })?;
        let mut profile = self.profile.clone();
        crate::tls::ech::prepare_options(
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

    #[cfg(feature = "quic")]
    pub async fn prepare_quic(
        &self,
        resolver: Option<&dyn EchConfigResolver>,
        alpn: &[Vec<u8>],
    ) -> io::Result<crate::quic::QuicClientConfig> {
        if self.prepared.lock().unwrap().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actual client material owner not started",
            ));
        }
        let mut profile = self.profile.clone();
        if matches!(
            profile.options.ech.source().map_err(io::Error::other)?,
            Some(zero_traits::EchConfigSource::Dns { .. })
        ) {
            let resolver = resolver.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "actual Node ECH resolver absent",
                )
            })?;
            crate::tls::ech::prepare_options(
                &mut profile.options,
                profile.server_name.as_deref(),
                &self.server,
                resolver,
            )
            .await?;
        }
        match &self.file {
            Some(file) => file.prepare_quic_material(&profile, alpn),
            None => crate::quic::client_config_with_ca_material(
                profile.insecure,
                None,
                alpn,
                None,
                None,
                &profile.options,
                None,
            )
            .map_err(io::Error::other),
        }
    }

    pub fn reuse_from_instance(
        &self,
        previous: ComponentInstanceId,
    ) -> io::Result<Option<Arc<[u8]>>> {
        let Some(previous) = self.census.previous(previous) else {
            return Ok(None);
        };
        if self.inherit(&previous)? {
            Ok(Some(self.stamped_identity()))
        } else {
            Ok(None)
        }
    }

    pub fn remember_cold_identity(self: &Arc<Self>, identity: &[u8]) -> io::Result<()> {
        if self.file.is_none() {
            return Ok(());
        }
        let mut cold = self.census.cold.lock().unwrap();
        cold.retain(|_, material| material.strong_count() != 0);
        cold.try_reserve(1).map_err(io::Error::other)?;
        cold.insert(identity.to_vec(), Arc::downgrade(self));
        Ok(())
    }

    pub fn reuse_from_cold_identity(&self, identity: &[u8]) -> io::Result<Option<Arc<[u8]>>> {
        let previous = {
            let cold = self.census.cold.lock().unwrap();
            cold.get(identity).and_then(std::sync::Weak::upgrade)
        };
        let Some(previous) = previous else {
            return Ok(None);
        };
        if self.inherit(&previous)? {
            Ok(Some(self.stamped_identity()))
        } else {
            Ok(None)
        }
    }

    fn inherit(&self, previous: &Self) -> io::Result<bool> {
        let Some(file) = &self.file else {
            return Ok(false);
        };
        let inherited = match &previous.file {
            Some(previous) => file.reuse_material_from(previous)?,
            None => false,
        };
        if inherited {
            self.stamp
                .store(previous.stamp.load(Ordering::Acquire), Ordering::Release);
        }
        Ok(inherited)
    }

    fn stamped_identity(&self) -> Arc<[u8]> {
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(&self.stamp.load(Ordering::Acquire).to_le_bytes());
        identity.into()
    }
}

/// Lifecycle owner that prepares `ClientMaterial` before activating `SessionOwner`.
pub struct ClientMaterialSessionOwner {
    pub session: SessionOwner,
    pub material: Option<Arc<ClientMaterial>>,
    pub instance: ComponentInstanceId,
    pub resources: Arc<dyn ResourceAccess>,
    pub cancel: CancellationToken,
}

impl ResourceLifecycle for ClientMaterialSessionOwner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), ChannelError> {
        if let Some(material) = &self.material {
            material
                .start(self.resources.as_ref(), self.instance, &self.cancel, end)
                .await?;
        }
        self.session.start(end).await?;
        if let Some(material) = &self.material {
            material.record(self.instance)?;
        }
        Ok(())
    }

    fn revoke(&self, reason: CloseReason) {
        self.session.revoke(reason);
    }

    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, ChannelError> {
        self.session.shutdown(end).await
    }
}

pub(super) fn component_error(instance: ComponentInstanceId) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, Reason::InvalidConfig, Impact::Component);
    error.component = Some(instance.component);
    error
}
