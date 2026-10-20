//! One actual candidate map, retained across cancellation until its originals end.
use super::{invalid, PendingEndpointMap, PendingLinkedEndpoint};
use std::{collections::HashMap, sync::Arc};
use zero_engine::EngineError;

type Entries = HashMap<String, Arc<PendingLinkedEndpoint>>;
pub(super) struct PendingEndpoints {
    owner: PendingEndpointMap,
    expected: Arc<Entries>,
    published: bool,
}
impl PendingEndpoints {
    pub(super) fn ensure_available(owner: &PendingEndpointMap) -> Result<(), EngineError> {
        if owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
        {
            return Err(invalid(
                "previous WireGuard candidate still owns its transition",
            ));
        }
        Ok(())
    }
    pub(super) fn install(
        owner: PendingEndpointMap,
        entries: Entries,
    ) -> Result<Self, EngineError> {
        let expected = Arc::new(entries);
        let mut current = owner.lock().unwrap_or_else(|error| error.into_inner());
        if current.is_some() {
            return Err(invalid(
                "previous WireGuard candidate still owns its transition",
            ));
        }
        *current = Some(expected.clone());
        drop(current);
        Ok(Self {
            owner,
            expected,
            published: false,
        })
    }
    /// The exact candidate has been cleaned (or was genuinely never started).
    /// Fence it even when empty so a delayed first poll cannot acquire later.
    pub(super) fn release_closed(
        owner: &PendingEndpointMap,
        expected: &Arc<Entries>,
    ) -> Result<(), EngineError> {
        for pending in expected.values() {
            pending.revoke();
        }
        let mut current = owner.lock().unwrap_or_else(|error| error.into_inner());
        if !current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            return Err(invalid(
                "WireGuard cleanup belongs to another actual candidate",
            ));
        }
        if expected.values().any(|pending| pending.needs_cleanup()) {
            return Err(invalid(
                "WireGuard candidate still retains original resources",
            ));
        }
        *current = None;
        Ok(())
    }
    /// Caller has published the exact ready Links, which now retain the same
    /// PeerResources. This does not stop, close or reconstruct any original.
    pub(super) fn published(&mut self) {
        self.published = true;
    }
}
impl Drop for PendingEndpoints {
    fn drop(&mut self) {
        if !self.published {
            for pending in self.expected.values() {
                pending.revoke();
            }
        }
        let mut current = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.expected))
            && (self.published
                || !self
                    .expected
                    .values()
                    .any(|pending| pending.needs_cleanup()))
        {
            *current = None;
        }
    }
}
#[cfg(test)]
mod tests;
