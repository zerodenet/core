//! Weak cold recipe views are metadata, never File/Scope/closure authority.
use super::*;
impl Options {
    /// Record the exact owning factory's final cold identity. The original
    /// options/plan keeps the material strong; this index cannot keep it alive.
    pub fn remember_cold_material_identity(&self, identity: &[u8]) -> std::io::Result<()> {
        let Some(material) = &self.material else {
            return Ok(());
        };
        if material.file.is_none() {
            return Ok(());
        }
        let mut cold = material.census.cold.lock().unwrap();
        cold.retain(|_, material| material.strong_count() != 0);
        cold.try_reserve(1).map_err(std::io::Error::other)?;
        cold.insert(identity.to_vec(), Arc::downgrade(material));
        Ok(())
    }
    /// Only the host's same-source pass may supply this exact previous owning
    /// factory identity. Matching the complete private recipe is independent:
    /// its Census owns this weak index. Empty snapshots remain empty and still
    /// require the original File grant; no path is read here.
    pub fn reuse_cold_file_material_from(&mut self, identity: &[u8]) -> std::io::Result<bool> {
        let Some(material) = &self.material else {
            return Ok(false);
        };
        let previous = {
            let cold = material.census.cold.lock().unwrap();
            cold.get(identity).and_then(std::sync::Weak::upgrade)
        };
        let Some(previous) = previous else {
            return Ok(false);
        };
        let inherited = material.inherit(&previous)?;
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
