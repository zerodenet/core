//! VLESS client TLS material adapter over [`zero_transport::tls::material`].
use super::Options;
use std::{num::NonZeroUsize, path::Path};
use zero_core::component::ComponentInstanceId;
pub(crate) use zero_transport::tls::material::{
    ClientMaterial as Material, ClientMaterialRegistry as Registry,
};

impl Options {
    pub fn with_owned_material(
        mut self,
        runtime: &crate::transport::VlessTransportRuntime,
        source_dir: Option<&Path>,
        max_bytes: NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        let Some(profile) = &self.tls else {
            return Ok(self);
        };
        let (material, identity) = Material::create(
            &runtime.client_material,
            profile,
            self.server.clone(),
            source_dir,
            max_bytes,
            &self.identity,
            b"vless.original-client-material.v1",
        )?;
        self.material = Some(material);
        self.identity = identity;
        Ok(self)
    }

    pub fn reuse_file_material_from(
        &mut self,
        previous: ComponentInstanceId,
    ) -> Result<bool, zero_transport::RuntimeError> {
        let Some(material) = &self.material else {
            return Ok(false);
        };
        if let Some(identity) = material.reuse_from_instance(previous)? {
            self.identity = identity;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn remember_cold_material_identity(&self, identity: &[u8]) -> std::io::Result<()> {
        match &self.material {
            Some(material) => material.remember_cold_identity(identity),
            None => Ok(()),
        }
    }

    pub fn reuse_cold_file_material_from(&mut self, identity: &[u8]) -> std::io::Result<bool> {
        let Some(material) = &self.material else {
            return Ok(false);
        };
        if let Some(updated) = material.reuse_from_cold_identity(identity)? {
            self.identity = updated;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
