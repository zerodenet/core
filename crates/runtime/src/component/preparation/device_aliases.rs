//! Only the actual published recovery pass may replace its concrete device aliases.
use super::*;
impl ResourcePreparation<'_> {
    pub(in crate::component) fn enable_device_alias_rebinding(mut self) -> Self {
        self.device_alias_rebinding = true;
        self
    }
    pub fn set_device_alias_overrides(
        &mut self,
        aliases: BTreeMap<Option<String>, ComponentRecipe>,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if !self.device_alias_rebinding || self.device_alias_overrides.is_some() {
            self.failed.set(true);
            return Err(invalid(
                Reason::StalePlan,
                "device alias overrides require the original recovery pass",
            ));
        }
        self.device_alias_overrides = Some(aliases);
        Ok(())
    }
    pub(in crate::component) fn take_device_alias_overrides(
        &mut self,
    ) -> BTreeMap<Option<String>, ComponentRecipe> {
        self.device_alias_overrides.take().unwrap_or_default()
    }
}
