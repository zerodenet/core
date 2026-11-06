//! Strong immutable CA snapshots belong to the actual complete cold recipe.
use super::*;
pub(in crate::grpc::owned) use crate::client_recipe::OwnedClientRecipe as CaFile;
use std::path::Path;
#[cfg(feature = "tls")]
mod cold;

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
            None => {
                static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
                self.file_stamp = NEXT
                    .try_update(
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                        |stamp| stamp.checked_add(1),
                    )
                    .map_err(|_| std::io::Error::other("gRPC cold material identity exhausted"))?;
                Arc::new(CaFile::new(path, source_dir, max_bytes.get())?)
            }
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
        if !ca.has_material() {
            return Err(error(instance, Stage::Prepare, Reason::StalePlan));
        }
        let recipe = self
            ._background_recipe
            .as_ref()
            .ok_or_else(|| error(instance, Stage::Prepare, Reason::StalePlan))?;
        let mut materials = recipe.materials.lock().unwrap();
        materials.retain(|(_, file, _)| file.strong_count() != 0);
        if let Some((_, actual, stamp)) =
            materials.iter().find(|(actual, _, _)| *actual == instance)
        {
            return if actual.ptr_eq(&Arc::downgrade(ca)) && *stamp == self.file_stamp {
                Ok(())
            } else {
                Err(error(instance, Stage::Prepare, Reason::StalePlan))
            };
        }
        materials.try_reserve(1).map_err(|source| {
            error(instance, Stage::Prepare, Reason::Capacity).with_source(source)
        })?;
        materials.push((instance, Arc::downgrade(ca), self.file_stamp));
        Ok(())
    }
    /// Copy immutable bytes only from this same complete pool recipe's actual
    /// installed previous instance. Host assembly separately proves same
    /// configuration identity; this grants no IO, reuse of tasks or closure.
    pub fn reuse_file_material_from(
        &mut self,
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
                .find(|(instance, _, _)| *instance == previous)
                .and_then(|(_, file, stamp)| file.upgrade().map(|file| (file, *stamp)))
        };
        let Some((previous, stamp)) = previous else {
            return Ok(false);
        };
        let inherited = ca.reuse_material_from(&previous)?;
        if inherited {
            self.file_stamp = stamp;
            self.refresh_identity();
        }
        Ok(inherited)
    }
}
