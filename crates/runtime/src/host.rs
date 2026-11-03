//! The single assembly owner for component lifetimes and accepted configuration work.
//! Protocol lookup and forwarding retain handles; they never create another host.
use crate::apply_owner::ApplyOwner;
use crate::component::{catalog::ComponentCatalog, ComponentRegistry};
use std::sync::{Arc, Mutex};
use zero_config::OutboundConfig;

pub struct RuntimeHost<A = ()> {
    components: ComponentCatalog,
    apply: Arc<ApplyOwner>,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    reload_ack: Mutex<Option<A>>,
}
impl<A> std::fmt::Debug for RuntimeHost<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHost")
            .field("components", &self.components)
            .field("apply", &self.apply)
            .finish_non_exhaustive()
    }
}
impl<A> RuntimeHost<A> {
    pub fn new(factories: ComponentRegistry<OutboundConfig>) -> Self {
        Self {
            components: ComponentCatalog::with_factories(factories),
            apply: Arc::default(),
            apply_lock: Arc::default(),
            reload_ack: Mutex::default(),
        }
    }
}

impl<A> RuntimeHost<A> {
    pub fn components(&self) -> &ComponentCatalog {
        &self.components
    }
    pub fn apply(&self) -> &Arc<ApplyOwner> {
        &self.apply
    }
    pub fn apply_lock(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.apply_lock
    }
    pub fn reload_ack(&self) -> &Mutex<Option<A>> {
        &self.reload_ack
    }
}

#[cfg(test)]
#[path = "tests/host.rs"]
mod tests;
