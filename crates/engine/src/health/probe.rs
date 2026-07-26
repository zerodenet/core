//! Runtime probe trigger registration.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A lightweight, type-erased callback for triggering an immediate urltest
/// probe cycle.  Created by the proxy and stored by the engine.
#[derive(Clone)]
pub struct ProbeTrigger {
    inner: Arc<dyn Fn() + Send + Sync>,
}

impl ProbeTrigger {
    pub fn new(f: impl Fn() + Send + Sync + 'static) -> Self {
        Self { inner: Arc::new(f) }
    }

    pub fn trigger(&self) {
        (self.inner)()
    }
}

/// Registry of probe triggers, keyed by group tag.
#[derive(Default)]
pub struct ProbeTriggerRegistry {
    triggers: Mutex<HashMap<String, ProbeTrigger>>,
}

impl std::fmt::Debug for ProbeTriggerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeTriggerRegistry")
            .field("triggers", &self.triggers.lock().unwrap().len())
            .finish()
    }
}

impl ProbeTriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: Mutex::new(HashMap::new()),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn register(&self, group_tag: &str, trigger: ProbeTrigger) {
        self.triggers
            .lock()
            .expect("probe trigger registry lock poisoned")
            .insert(group_tag.to_owned(), trigger);
    }

    pub fn remove(&self, group_tag: &str) {
        self.triggers
            .lock()
            .expect("probe trigger registry lock poisoned")
            .remove(group_tag);
    }

    pub fn clear(&self) {
        self.triggers
            .lock()
            .expect("probe trigger registry lock poisoned")
            .clear();
    }

    pub fn get(&self, group_tag: &str) -> Option<ProbeTrigger> {
        self.triggers
            .lock()
            .expect("probe trigger registry lock poisoned")
            .get(group_tag)
            .cloned()
    }
}
