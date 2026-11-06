//! Full cold identities point weakly at the original immutable recipe only.
use super::*;
impl NativePoolOptions {
    /// Called after all original factory options/dependencies are finalized.
    /// The actual cold plan retains the file; this index is not File authority.
    pub fn remember_cold_file_material(&self) -> Result<(), RuntimeError> {
        let (Some(ca), Some(recipe)) = (&self.ca, &self._background_recipe) else {
            return Ok(());
        };
        let mut cold = recipe.cold_materials.lock().unwrap();
        cold.retain(|_, (file, _)| file.strong_count() != 0);
        cold.try_reserve(1).map_err(std::io::Error::other)?;
        cold.insert(
            self.identity.to_vec(),
            (Arc::downgrade(ca), self.file_stamp),
        );
        Ok(())
    }
    /// The host independently proves same Source and supplies the exact
    /// previous owning Slot identity, possibly from an unbuilt cold plan.
    /// Matching complete Recipe and stamp never substitutes for actual bytes.
    pub fn reuse_cold_file_material_from(&mut self, identity: &[u8]) -> Result<bool, RuntimeError> {
        let (Some(ca), Some(recipe)) = (&self.ca, &self._background_recipe) else {
            return Ok(false);
        };
        let previous = {
            let cold = recipe.cold_materials.lock().unwrap();
            cold.get(identity)
                .and_then(|(file, stamp)| file.upgrade().map(|file| (file, *stamp)))
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
