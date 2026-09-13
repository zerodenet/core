use super::Portal;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Shared by authenticated listener profiles and prepared virtual outbounds.
#[derive(Clone, Default, Debug)]
pub struct PortalRegistry(Arc<Mutex<HashMap<String, Portal>>>);
impl PortalRegistry {
    pub fn portal(&self, tag: &str) -> Portal {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(tag.to_owned())
            .or_default()
            .clone()
    }
    /// Retired leaves cannot attach workers or recover a replacement pool.
    pub fn retire(&self) {
        for (_, portal) in self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .drain()
        {
            portal.retire();
        }
    }
}
