//! A note from the currently executing typed named decoder, not a new hook.
use super::*;
impl ResourcePreparation<'_> {
    /// Called after the actual factory options/dependencies/material have been
    /// prepared once. The exact description stamp checks only consistency; it
    /// grants no role, Scope, native permission or old-owner retirement rights.
    pub fn declare_named_packet_path(
        &mut self,
        tag: &str,
        expected_identity: Arc<[u8]>,
        facts: super::super::PacketPathFacts,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if self.active_named.as_deref() != Some(tag)
            || !self.named_components.contains_key(tag)
            || self.named_packet_paths.contains_key(tag)
            || facts.endpoint.port == 0
        {
            self.invalidate();
            return Err(invalid(
                Reason::StalePlan,
                "packet path facts do not belong to the active named recipe",
            ));
        }
        self.named_packet_paths
            .insert(tag.to_owned(), (expected_identity, facts));
        Ok(())
    }
    /// Reuse the same actual recipe, including recovery's immutable accepted
    /// materials. This cannot invoke another old protocol claim/CA decoder.
    pub fn prepared_named_packet_path(
        &mut self,
        tag: &str,
    ) -> Result<Option<super::super::PacketPathFacts>, BuildError> {
        self.prepare_named_component(tag)?;
        Ok(self
            .named_recipes
            .get(tag)
            .and_then(|(recipe, _)| recipe.packet_path_facts())
            .cloned())
    }
}
