//! Whole-assembly reuse observes the same cold native declarations as build.
use super::*;
fn domain(config: &zero_config::RuntimeConfig, names: &[String]) -> BTreeSet<String> {
    fn collect(
        config: &zero_config::RuntimeConfig,
        name: &str,
        active: &mut BTreeSet<String>,
        out: &mut BTreeSet<String>,
    ) {
        if config.outbounds.iter().any(|input| input.tag == name) {
            out.insert(name.into());
            return;
        }
        if !active.insert(name.into()) {
            return;
        }
        if let Some(group) = config
            .outbound_groups
            .iter()
            .find(|group| group.tag == name)
        {
            for child in group.group.members() {
                collect(config, child, active, out);
            }
        }
        active.remove(name);
    }
    let mut selected: BTreeSet<_> = names.iter().cloned().collect();
    loop {
        let before = selected.len();
        for group in &config.outbound_groups {
            if !matches!(&group.group, zero_config::OutboundGroupKind::Relay { .. }) {
                continue;
            }
            let mut members = BTreeSet::new();
            collect(config, &group.tag, &mut BTreeSet::new(), &mut members);
            if !members.is_disjoint(&selected) {
                selected.extend(members);
            }
        }
        if before == selected.len() {
            return selected;
        }
    }
}
impl ResourcePreparation<'_> {
    /// Materialize other retained members before a one-use device chooses reuse.
    /// This is the existing memoized cold pass, never another assembly or IO grant.
    pub fn prepare_lifecycle_domain(&mut self, names: &[String]) -> Result<(), BuildError> {
        self.ensure_valid()?;
        let Some(config) = self.candidate else {
            return Ok(());
        };
        let members = domain(config, names);
        let prepare: Vec<_> = members
            .into_iter()
            .filter(|tag| {
                self.named_component(tag).is_some()
                    && !self.preparing_named.contains(tag)
                    && self
                        .previous_named
                        .and_then(|old| old.get(tag))
                        .is_some_and(|slot| slot.recipe.lifecycle_owner().is_some())
            })
            .collect();
        for tag in prepare {
            self.prepare_named_component(&tag)?;
        }
        Ok(())
    }
    pub(super) fn lifecycle_reuse_members(
        &self,
        id: ComponentId,
    ) -> Option<Vec<zero_core::component::ComponentInstanceId>> {
        let previous = self.previous_slots.iter().find(|slot| slot.id == id)?;
        let group = previous.lifecycle_group();
        let Some(config) = self.candidate else {
            return group.is_none().then(Vec::new);
        };
        let owners = self.candidate_lifecycle_owners(config)?;
        let Some(group) = group else {
            // Introducing a grouped lifecycle changes the actual cold owner,
            // even when the underlying transport description is unchanged.
            return (!owners.contains_key(&id)).then(Vec::new);
        };
        let owner = owners.get(&id)?;
        let actual: BTreeSet<_> = owners
            .iter()
            .filter_map(|(id, root)| (root == owner).then_some(*id))
            .collect();
        let expected: BTreeSet<_> = group.iter().map(|member| member.component).collect();
        (actual == expected).then(|| group.to_vec())
    }
    fn candidate_lifecycle_owners(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Option<BTreeMap<ComponentId, ComponentId>> {
        let nodes = self
            .resources
            .values()
            .map(|entry| (entry.component, &entry.recipe))
            .chain(self.named_recipes.iter().filter_map(|(tag, (recipe, _))| {
                self.named_components.get(tag).map(|id| (*id, recipe))
            }));
        crate::component::catalog::cold::lifecycle::owners(nodes, &self.named_components, config)
            .ok()
    }
}
