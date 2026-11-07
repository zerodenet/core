//! Typed cold preparation of canonical roleless resource owners.
mod accounting;
mod device_aliases;
mod identities;
mod named;
mod packet_path;
mod source_dependencies;
use super::{dependencies, instance::invalid, ComponentRecipe};
pub use identities::SharedResourceIdentities;
pub use source_dependencies::SourceDependencyDeclarations;
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use zero_core::component::ComponentId;
use zero_platform_tokio::contract::{
    component::{ComponentFactory, ComponentSpec},
    lifecycle::BuildError,
    ChannelError, Reason,
};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    namespace: &'static str,
    identity: Arc<[u8]>,
    accounting_limit: Option<usize>,
    process_limits: Option<super::recipe::accounting::ProcessLimits>,
    preparation_limits: Option<super::recipe::accounting::PreparationLimits>,
    buffer_limits: Option<zero_platform_tokio::controlled_carrier::CarrierBufferLimits>,
    component_task_total: Option<usize>,
}

/// A real host-assigned identity and its state-only resource recipe.
#[derive(Clone)]
pub struct PreparedSharedResource {
    component: ComponentId,
    recipe: ComponentRecipe,
    fresh: bool,
}
impl PreparedSharedResource {
    pub fn component(&self) -> ComponentId {
        self.component
    }
    pub fn recipe(&self) -> &ComponentRecipe {
        &self.recipe
    }
    pub(in crate::component) fn requires_fresh_instance(&self) -> bool {
        self.fresh
    }
}

/// One candidate preparation pass. The host supplies its actual allocator.
/// This object never starts resources or publishes a candidate.
pub struct ResourcePreparation<'a> {
    allocate: &'a mut dyn FnMut() -> Result<ComponentId, BuildError>,
    builtin_component: Option<ComponentId>,
    named_components: BTreeMap<String, ComponentId>,
    primary_factories: Option<&'a super::ComponentRegistry<zero_config::OutboundConfig>>,
    named_recipes: BTreeMap<String, (ComponentRecipe, Option<super::ValidatedRecipe>)>,
    preparing_named: BTreeSet<String>,
    active_named: Option<String>,
    named_packet_paths: BTreeMap<String, (Arc<[u8]>, super::PacketPathFacts)>,
    source_dependencies: Option<SourceDependencyDeclarations>,
    resources: BTreeMap<Key, PreparedSharedResource>,
    identities: BTreeSet<ComponentId>,
    failed: Cell<bool>,
    previous: SharedResourceIdentities,
    previous_slots: &'a [Arc<super::instance::ComponentSlot>],
    previous_named: Option<&'a BTreeMap<String, Arc<super::instance::ComponentSlot>>>,
    previous_loans: Vec<(
        zero_core::component::ComponentInstanceId,
        zero_core::component::ComponentInstanceId,
    )>,
    candidate: Option<&'a zero_config::RuntimeConfig>,
    same_source: bool,
    restarting: bool,
    device_alias_rebinding: bool,
    device_alias_overrides: Option<BTreeMap<Option<String>, ComponentRecipe>>,
    source_dir: Option<&'a std::path::Path>,
}
impl<'a> ResourcePreparation<'a> {
    pub fn new(allocate: &'a mut dyn FnMut() -> Result<ComponentId, BuildError>) -> Self {
        Self {
            allocate,
            builtin_component: None,
            named_components: BTreeMap::new(),
            primary_factories: None,
            named_recipes: BTreeMap::new(),
            preparing_named: BTreeSet::new(),
            active_named: None,
            named_packet_paths: BTreeMap::new(),
            source_dependencies: None,
            resources: BTreeMap::new(),
            identities: BTreeSet::new(),
            failed: Cell::new(false),
            previous: SharedResourceIdentities::default(),
            previous_slots: &[],
            previous_named: None,
            previous_loans: Vec::new(),
            candidate: None,
            same_source: false,
            restarting: false,
            device_alias_rebinding: false,
            device_alias_overrides: None,
            source_dir: None,
        }
    }

    pub(super) fn with_previous_slots(
        mut self,
        previous: &'a [Arc<super::instance::ComponentSlot>],
        loans: impl IntoIterator<
            Item = (
                zero_core::component::ComponentInstanceId,
                zero_core::component::ComponentInstanceId,
            ),
        >,
    ) -> Self {
        self.previous_slots = previous;
        self.previous_loans = loans.into_iter().collect();
        self
    }
    pub(in crate::component) fn with_restart_resources(mut self, restarting: bool) -> Self {
        self.restarting = restarting;
        self
    }
    /// Read-only evidence from this host's actual previous slot. A dependency
    /// must already have been requested in this very pass with the same spec
    /// and exact healthy dependency instances. Unknown dependencies deny reuse.
    /// This never grants admission or constructs a replacement owner.
    pub fn reusable_resource_instance(
        &self,
        component: ComponentId,
    ) -> Option<zero_core::component::ComponentInstanceId> {
        fn check(
            preparation: &ResourcePreparation<'_>,
            id: ComponentId,
            seen: &mut BTreeSet<ComponentId>,
        ) -> Option<zero_core::component::ComponentInstanceId> {
            if !seen.insert(id) {
                return None;
            }
            let shared = preparation
                .resources
                .values()
                .find(|node| node.component == id);
            let (recipe, fresh) = match shared {
                Some(candidate) => (&candidate.recipe, candidate.fresh),
                None => {
                    let tag = preparation
                        .named_components
                        .iter()
                        .find(|(_, actual)| **actual == id)?
                        .0;
                    let (recipe, _) = preparation.named_recipes.get(tag)?;
                    (recipe, false)
                }
            };
            let old = preparation
                .previous_slots
                .iter()
                .find(|node| node.id == id)?;
            if fresh
                || !old.can_reuse()
                || recipe.spec() != old.recipe.spec()
                || recipe.packet_path_facts() != old.recipe.packet_path_facts()
                || recipe.carrier_total_limit() != old.recipe.carrier_total_limit()
                || recipe.process_limits() != old.recipe.process_limits()
                || recipe.preparation_limits() != old.recipe.preparation_limits()
                || recipe.buffer_limits() != old.recipe.buffer_limits()
                || recipe.component_task_total() != old.recipe.component_task_total()
            {
                return None;
            }
            for dependency in old.dependency_instances().iter().chain(
                preparation
                    .previous_loans
                    .iter()
                    .filter(|(from, _)| *from == old.instance_id())
                    .map(|(_, to)| to),
            ) {
                if check(preparation, dependency.component, seen)? != *dependency {
                    return None;
                }
            }
            seen.remove(&id);
            Some(old.instance_id())
        }
        self.ensure_valid().ok()?;
        if self.restarting {
            return None;
        }
        check(self, component, &mut BTreeSet::new())
    }

    /// Read-only reuse evidence for a one-use recipe before its owner chooses
    /// fresh Options. Dependencies/loans must already be requested in this pass.
    /// The scope is the original factory's weak metadata, never authority.
    pub fn can_reuse_shared_resource(
        &self,
        namespace: &'static str,
        spec: &ComponentSpec,
        scope: Option<&zero_platform_tokio::contract::scope::TaskScopeIdentity>,
    ) -> bool {
        if self.restarting || self.ensure_valid().is_err() {
            return false;
        }
        let Ok(accounting_limit) = self.carrier_total_limit_for(spec) else {
            return false;
        };
        let Ok(process_limits) = self.process_limits_for(spec) else {
            return false;
        };
        let Ok(preparation_limits) = self.preparation_limits_for(spec) else {
            return false;
        };
        let Ok(buffer_limits) = self.buffer_limits_for(spec) else {
            return false;
        };
        let Ok(component_task_total) = self.component_task_total_for(spec) else {
            return false;
        };
        let key = Key {
            namespace,
            accounting_limit,
            process_limits,
            preparation_limits,
            buffer_limits,
            component_task_total,
            identity: spec.static_identity.clone(),
        };
        if self
            .resources
            .get(&key)
            .is_some_and(|resource| resource.fresh)
        {
            return false;
        }
        let Some(id) = self.previous.entries.get(&key) else {
            return false;
        };
        let Some(old) = self.previous_slots.iter().find(|slot| slot.id == *id) else {
            return false;
        };
        if !old.can_reuse()
            || old.recipe.spec() != spec
            || old.recipe.carrier_total_limit() != accounting_limit
            || old.recipe.process_limits() != process_limits
            || old.recipe.preparation_limits() != preparation_limits
            || old.recipe.buffer_limits() != buffer_limits
            || old.recipe.component_task_total() != component_task_total
            || !old.matches_scope_identity(scope)
        {
            return false;
        }
        old.dependency_instances()
            .iter()
            .chain(
                self.previous_loans
                    .iter()
                    .filter(|(from, _)| *from == old.instance_id())
                    .map(|(_, to)| to),
            )
            .all(|dependency| {
                self.reusable_resource_instance(dependency.component) == Some(*dependency)
            })
    }

    /// Borrow the whole current candidate for assembly-owned shared profile sets.
    /// This reference cannot escape this cold pass into a resource recipe.
    pub fn with_candidate(mut self, candidate: &'a zero_config::RuntimeConfig) -> Self {
        self.source_dir = candidate.source_dir();
        self.candidate = Some(candidate);
        self
    }
    /// Only actual host preparation compares its simultaneously live inputs.
    /// Factories receive observation, never a source token or I/O authority.
    pub(in crate::component) fn with_same_source_as_previous(mut self, same: bool) -> Self {
        self.same_source = same;
        self
    }
    pub fn same_source_as_previous(&self) -> bool {
        self.ensure_valid().is_ok() && self.same_source
    }
    /// Previous actual instance only. This deliberately says nothing about
    /// liveness, reusable tasks, native/user closure or available admission.
    pub fn previous_resource_instance(
        &self,
        component: ComponentId,
    ) -> Option<zero_core::component::ComponentInstanceId> {
        self.ensure_valid().ok()?;
        self.previous_slots
            .iter()
            .find(|slot| slot.id == component)
            .map(|slot| slot.instance_id())
    }
    /// Opaque identity of the actual previous Slot, for the owning protocol's
    /// same-source immutable cold recipe lookup only. This cannot establish
    /// liveness, material authorization, native acquisition or cleanup.
    pub fn previous_resource_identity(&self, component: ComponentId) -> Option<&[u8]> {
        self.ensure_valid().ok()?;
        Some(
            self.previous_slots
                .iter()
                .find(|slot| slot.id == component)?
                .recipe
                .spec()
                .static_identity
                .as_ref(),
        )
    }
    pub(super) fn with_previous_named(
        mut self,
        previous: Option<&'a BTreeMap<String, Arc<super::instance::ComponentSlot>>>,
    ) -> Self {
        self.previous_named = previous;
        self
    }
    /// Metadata from the host's actual previous named Slot. This never
    /// selects by profile/path, grants IO, or confirms Source/Scope reuse.
    /// Factories must separately prove the full recipe and same Source before
    /// copying that actual instance's already authorized immutable material.
    pub fn previous_named_instance(
        &self,
        tag: &str,
    ) -> Option<zero_core::component::ComponentInstanceId> {
        self.ensure_valid().ok()?;
        self.previous_named?.get(tag).map(|slot| slot.instance_id())
    }
    /// Exact direct dependencies of the actual previously published named Slot.
    /// Read-only metadata, never a recipe match, reuse grant or close receipt.
    pub fn previous_named_dependencies(
        &self,
        tag: &str,
    ) -> Option<&[zero_core::component::ComponentInstanceId]> {
        self.ensure_valid().ok()?;
        Some(self.previous_named?.get(tag)?.dependency_instances())
    }
    /// Borrow only for this cold pass; owned protocol profiles resolve their
    /// own paths and cannot retain a previous configuration document.
    pub fn with_source_dir(mut self, directory: Option<&'a std::path::Path>) -> Self {
        self.source_dir = directory;
        self
    }
    pub fn source_dir(&self) -> Option<&std::path::Path> {
        self.source_dir
    }
    pub fn candidate(&self) -> Option<&zero_config::RuntimeConfig> {
        self.candidate
    }

    /// Recovery uses the actual published immutable recipes, never rereads
    /// mutable source files. Deferred device recipes are rebuilt by their real
    /// caller after original cleanup because their one-use stacks were consumed.
    pub(in crate::component) fn inherit_canonical_resources(
        &mut self,
        slots: &[Arc<super::instance::ComponentSlot>],
        deferred: &BTreeSet<ComponentId>,
    ) -> Result<(), BuildError> {
        for (key, id) in &self.previous.entries {
            if deferred.contains(id) {
                continue;
            }
            let slot = slots.iter().find(|slot| slot.id == *id).ok_or_else(|| {
                invalid(
                    Reason::InvalidConfig,
                    "published canonical resource has no original recipe",
                )
            })?;
            if slot.recipe.spec().static_identity != key.identity {
                return Err(invalid(
                    Reason::InvalidConfig,
                    "published canonical resource identity differs",
                ));
            }
            if !self.identities.insert(*id) {
                return Err(invalid(
                    Reason::InvalidConfig,
                    "duplicate original canonical resource",
                ));
            }
            self.resources.insert(
                key.clone(),
                PreparedSharedResource {
                    component: *id,
                    recipe: slot.recipe.clone(),
                    fresh: false,
                },
            );
        }
        Ok(())
    }

    /// Namespace identifies one owning factory/state within this assembly.
    /// Options come from that same actual protocol state. Identity is the
    /// factory's validated secret-free static identity, never a config document.
    pub fn share<F: ComponentFactory>(
        &mut self,
        namespace: &'static str,
        factory: Arc<F>,
        options: Arc<F::Options>,
    ) -> Result<ComponentId, BuildError> {
        self.ensure_valid()?;
        let recipe = ComponentRecipe::from_validated(factory, options);
        if !recipe.spec().roles.is_empty() {
            self.invalidate();
            return Err(invalid(
                Reason::InvalidConfig,
                "shared resource recipe must declare its actual roleless owner",
            ));
        }
        let result = self.prepare_validated(namespace, recipe);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    /// A one-use original recipe was rebuilt by its owner in this cold pass.
    /// Keep its canonical ID, but never substitute a same-spec previous slot:
    /// that slot retains a different original Options/pending resource owner.
    /// This creates no resource, admission, task, or replacement-close proof.
    pub fn require_fresh_resource(&mut self, component: ComponentId) -> Result<(), BuildError> {
        self.ensure_valid()?;
        if let Some(resource) = self
            .resources
            .values_mut()
            .find(|node| node.component == component)
        {
            resource.fresh = true;
            return Ok(());
        }
        self.invalidate();
        let mut error = invalid(
            Reason::InvalidConfig,
            "fresh resource was not requested in this actual cold pass",
        );
        error.component = Some(component);
        Err(error)
    }

    /// Add one typed ordinary component to this same cold union and allocator.
    /// Catalog role/provider validation and actual StopOld remain mandatory.
    pub fn prepare_component<F: ComponentFactory>(
        &mut self,
        namespace: &'static str,
        factory: Arc<F>,
        options: Arc<F::Options>,
    ) -> Result<ComponentId, BuildError> {
        self.prepare_component_recipe(namespace, ComponentRecipe::from_validated(factory, options))
    }
    /// Preserve the same typed validated recipe's concrete host native bindings
    /// while adding it to this pass's one namespace/allocator and cold graph.
    pub fn prepare_component_recipe(
        &mut self,
        namespace: &'static str,
        recipe: ComponentRecipe,
    ) -> Result<ComponentId, BuildError> {
        self.ensure_valid()?;
        let result = self.prepare_validated(namespace, recipe);
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    fn prepare_validated(
        &mut self,
        namespace: &'static str,
        recipe: ComponentRecipe,
    ) -> Result<ComponentId, BuildError> {
        if namespace.is_empty() {
            return Err(invalid(
                Reason::InvalidConfig,
                "component namespace is empty",
            ));
        }
        let recipe = self.project_carrier_total(recipe)?;
        let key = Key {
            namespace,
            accounting_limit: recipe.carrier_total_limit(),
            process_limits: recipe.process_limits(),
            preparation_limits: recipe.preparation_limits(),
            buffer_limits: recipe.buffer_limits(),
            component_task_total: recipe.component_task_total(),
            identity: recipe.spec().static_identity.clone(),
        };
        if let Some(existing) = self.resources.get(&key) {
            if existing.recipe.spec() != recipe.spec() {
                let mut error = invalid(
                    Reason::InvalidConfig,
                    "conflicting shared resource description",
                );
                error.component = Some(existing.component);
                return Err(error);
            }
            return Ok(existing.component);
        }
        let component = match self.previous.entries.get(&key) {
            Some(&id) => id,
            None => (self.allocate)()?,
        };
        if !self.identities.insert(component) {
            let mut error = invalid(
                Reason::InvalidConfig,
                "host allocator repeated a component identity",
            );
            error.component = Some(component);
            return Err(error);
        }
        self.resources.insert(
            key,
            PreparedSharedResource {
                component,
                recipe,
                fresh: false,
            },
        );
        Ok(component)
    }

    pub fn resources(&self) -> impl Iterator<Item = &PreparedSharedResource> {
        self.resources.values()
    }

    /// Validate the whole candidate, including its actual primary component IDs.
    /// This establishes ordering only; the host still owns replacement and I/O.
    pub fn validate<'s>(
        &'s self,
        primary: impl IntoIterator<Item = (ComponentId, &'s ComponentSpec)>,
    ) -> Result<Vec<ComponentId>, ChannelError> {
        self.ensure_valid()?;
        let result = dependencies::order(
            primary.into_iter().chain(
                self.resources
                    .values()
                    .map(|resource| (resource.component, resource.recipe.spec())),
            ),
        );
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    /// A failed consumer invalidates the whole pass. Discard it and prepare a
    /// fresh candidate; allocated identities remain consumed by the host.
    pub fn into_resources(self) -> Result<Vec<PreparedSharedResource>, BuildError> {
        self.ensure_valid()?;
        let mut resources: Vec<_> = self.resources.into_values().collect();
        resources.sort_by_key(|resource| resource.component);
        Ok(resources)
    }

    pub(super) fn invalidate(&self) {
        self.failed.set(true);
    }

    pub(super) fn ensure_valid(&self) -> Result<(), BuildError> {
        if self.failed.get() {
            return Err(invalid(
                Reason::InvalidConfig,
                "resource preparation pass failed",
            ));
        }
        Ok(())
    }
}
