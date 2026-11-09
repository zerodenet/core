//! Actual primary IDs reserved once by the same cold catalog pass.
use super::*;
impl ResourcePreparation<'_> {
    pub(in crate::component) fn reserve_primary_components(
        &mut self,
        builtin: Option<ComponentId>,
        named: impl IntoIterator<Item = (String, Option<ComponentId>)>,
    ) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if self.builtin_component.is_some() || !self.named_components.is_empty() {
            return Err(invalid(
                Reason::StalePlan,
                "primary identities already reserved",
            ));
        }
        self.builtin_component = Some(match builtin {
            Some(id) => id,
            None => (self.allocate)()?,
        });
        for (tag, old) in named {
            let id = match old {
                Some(id) => id,
                None => (self.allocate)()?,
            };
            if self.named_components.insert(tag, id).is_some() {
                self.failed.set(true);
                return Err(invalid(
                    Reason::InvalidConfig,
                    "duplicate primary component name",
                ));
            }
        }
        Ok(())
    }
    /// This pass's real named component ID, including a forward dependency.
    /// No generation, material, build/admission or native authority is inferred.
    pub fn named_component(&self, tag: &str) -> Option<ComponentId> {
        self.ensure_valid().ok()?;
        self.named_components.get(tag).copied()
    }
    pub fn builtin_component(&self) -> Option<ComponentId> {
        self.ensure_valid().ok()?;
        self.builtin_component
    }
}

impl<'a> ResourcePreparation<'a> {
    pub(in crate::component) fn with_primary_factories(
        mut self,
        factories: &'a super::super::ComponentRegistry<zero_config::OutboundConfig>,
    ) -> Self {
        self.primary_factories = Some(factories);
        self
    }
    /// Immutable description from this same pass's already memoized recipe.
    /// Recovery reads only its accepted recipe. This supplies no IO/role rights.
    pub fn prepared_named_identity(&mut self, tag: &str) -> Result<Arc<[u8]>, BuildError> {
        self.prepare_named_component(tag)?;
        self.named_recipes
            .get(tag)
            .map(|(recipe, _)| recipe.spec().static_identity.clone())
            .ok_or_else(|| invalid(Reason::StalePlan, "same named recipe was not retained"))
    }
    /// A device's cold semantic projection can decline an unused alias before
    /// its actual primary decoder runs. The source keeps its existing executor;
    /// a prepared or referenced actual component cannot be removed this way.
    pub fn omit_unused_device_alias(&mut self, tag: &str) -> Result<(), BuildError> {
        self.ensure_valid()?;
        let id =
            self.named_components.get(tag).copied().ok_or_else(|| {
                invalid(Reason::StalePlan, "device alias has no reserved identity")
            })?;
        if self.device_alias_rebinding
            || self.active_named.is_some()
            || !self.preparing_named.is_empty()
            || self.named_recipes.contains_key(tag)
            || self
                .source_dependencies
                .as_ref()
                .is_some_and(|sources| sources.values().any(|(_, ids)| ids.contains(&id)))
            || self
                .resources
                .values()
                .any(|resource| resource.recipe.spec().dependencies.contains(&id))
            || self
                .named_recipes
                .values()
                .any(|(recipe, _)| recipe.spec().dependencies.contains(&id))
        {
            self.invalidate();
            return Err(invalid(
                Reason::StalePlan,
                "prepared/referenced device alias cannot become unselected",
            ));
        }
        self.named_components.remove(tag);
        Ok(())
    }
    /// Prepare this existing named callee exactly once in the same cold pass.
    /// The actual protocol decoder keeps all private options/material. No owner
    /// starts here, and a cyclic request invalidates this entire candidate.
    pub fn prepare_named_component(&mut self, tag: &str) -> Result<ComponentId, BuildError> {
        self.ensure_valid()?;
        let id = self.named_component(tag).ok_or_else(|| {
            invalid(
                Reason::InvalidConfig,
                "named callee has no actual reserved identity",
            )
        })?;
        if self.named_recipes.contains_key(tag) {
            return Ok(id);
        }
        if !self.preparing_named.insert(tag.to_owned()) {
            self.invalidate();
            return Err(invalid(
                Reason::InvalidConfig,
                "cyclic named cold preparation",
            ));
        }
        let previous_active = self.active_named.replace(tag.to_owned());
        let result = (|| {
            if self.device_alias_rebinding {
                // Recovery consumes only the actual accepted immutable recipe;
                // material paths and protocol configuration are not re-decoded.
                let old = self
                    .previous_named
                    .and_then(|named| named.get(tag))
                    .ok_or_else(|| {
                        invalid(Reason::StalePlan, "accepted named recipe is unavailable")
                    })?;
                if old.id != id {
                    return Err(invalid(
                        Reason::StalePlan,
                        "accepted named identity changed",
                    ));
                }
                self.named_recipes
                    .insert(tag.to_owned(), (old.recipe.clone(), None));
            } else {
                let factories = self.primary_factories.ok_or_else(|| {
                    invalid(
                        Reason::Unsupported,
                        "named preparation requires the actual catalog factories",
                    )
                })?;
                let input = self
                    .candidate
                    .and_then(|config| config.outbounds.iter().find(|input| input.tag == tag))
                    .ok_or_else(|| {
                        invalid(
                            Reason::InvalidConfig,
                            "named callee is absent from this actual candidate",
                        )
                    })?;
                let mut validated = factories
                    .prepare_with_resources(input.protocol.protocol_name(), input, self)
                    .map_err(|source| {
                        invalid(Reason::InvalidConfig, "named callee preparation failed")
                            .with_source(source)
                    })?;
                if let Some((identity, facts)) = self.named_packet_paths.remove(tag) {
                    if identity.as_ref() != validated.spec().static_identity.as_ref() {
                        return Err(invalid(
                            Reason::StalePlan,
                            "packet path facts differ from their actual prepared recipe",
                        ));
                    }
                    validated = validated.with_packet_path_facts(facts);
                }
                self.named_recipes.insert(
                    tag.to_owned(),
                    (validated.component_recipe(), Some(validated)),
                );
            }
            Ok(id)
        })();
        self.active_named = previous_active;
        self.preparing_named.remove(tag);
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    pub(in crate::component) fn prepared_named_recipe(
        &mut self,
        tag: &str,
    ) -> Result<super::super::ValidatedRecipe, BuildError> {
        self.prepare_named_component(tag)?;
        self.named_recipes
            .get(tag)
            .and_then(|(_, validated)| validated.clone())
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "recovery named recipe cannot enter a new decode path",
                )
            })
    }
}
