//! Typed cold preparation of canonical roleless resource owners.
use super::{dependencies, instance::invalid, ComponentRecipe};
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
}

/// A real host-assigned identity and its state-only resource recipe.
#[derive(Clone)]
pub struct PreparedSharedResource {
    component: ComponentId,
    recipe: ComponentRecipe,
}
impl PreparedSharedResource {
    pub fn component(&self) -> ComponentId {
        self.component
    }
    pub fn recipe(&self) -> &ComponentRecipe {
        &self.recipe
    }
}

/// One candidate preparation pass. The host supplies its actual allocator.
/// This object never starts resources or publishes a candidate.
pub struct ResourcePreparation<'a> {
    allocate: &'a mut dyn FnMut() -> Result<ComponentId, BuildError>,
    resources: BTreeMap<Key, PreparedSharedResource>,
    identities: BTreeSet<ComponentId>,
    failed: Cell<bool>,
}
impl<'a> ResourcePreparation<'a> {
    pub fn new(allocate: &'a mut dyn FnMut() -> Result<ComponentId, BuildError>) -> Self {
        Self {
            allocate,
            resources: BTreeMap::new(),
            identities: BTreeSet::new(),
            failed: Cell::new(false),
        }
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
        let result = self.share_validated(namespace, factory, options);
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn share_validated<F: ComponentFactory>(
        &mut self,
        namespace: &'static str,
        factory: Arc<F>,
        options: Arc<F::Options>,
    ) -> Result<ComponentId, BuildError> {
        if namespace.is_empty() {
            return Err(invalid(
                Reason::InvalidConfig,
                "shared resource namespace is empty",
            ));
        }
        let recipe = ComponentRecipe::from_validated(factory, options);
        if !recipe.spec().roles.is_empty() {
            return Err(invalid(
                Reason::InvalidConfig,
                "shared resource recipe must declare its actual roleless owner",
            ));
        }
        let key = Key {
            namespace,
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
        let component = (self.allocate)()?;
        if !self.identities.insert(component) {
            let mut error = invalid(
                Reason::InvalidConfig,
                "host allocator repeated a component identity",
            );
            error.component = Some(component);
            return Err(error);
        }
        self.resources
            .insert(key, PreparedSharedResource { component, recipe });
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
