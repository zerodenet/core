//! One actual candidate map, retained across cancellation until its originals end.
use super::{invalid, PendingEndpointMap, PendingLinkedEndpoint};
use std::{collections::HashMap, sync::Arc};
use zero_engine::EngineError;

pub(in crate::adapters::wireguard) struct PendingEntries {
    entries: HashMap<String, Arc<PendingLinkedEndpoint>>,
    candidate: Option<std::sync::Weak<zero_config::RuntimeConfig>>,
    pub(super) traffic: std::sync::Mutex<HashMap<String, zero_engine::PreparedEndpointTraffic>>,
    pub(super) invocations:
        tokio::sync::Mutex<HashMap<String, super::recipe::retained::RetainedInvocation>>,
}
impl PendingEntries {
    pub(in crate::adapters::wireguard) fn new(
        entries: HashMap<String, Arc<PendingLinkedEndpoint>>,
    ) -> Self {
        Self {
            entries,
            candidate: None,
            traffic: std::sync::Mutex::new(HashMap::new()),
            invocations: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
    pub(super) fn candidate_matches(&self, config: &zero_config::RuntimeConfig) -> bool {
        self.candidate
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|candidate| std::ptr::eq(candidate.as_ref(), config))
    }
    pub(super) fn cold(
        config: &Arc<zero_config::RuntimeConfig>,
        entries: HashMap<String, Arc<PendingLinkedEndpoint>>,
    ) -> Self {
        let mut candidate = Self::new(entries);
        candidate.candidate = Some(Arc::downgrade(config));
        candidate
    }
}
impl std::ops::Deref for PendingEntries {
    type Target = HashMap<String, Arc<PendingLinkedEndpoint>>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
type Entries = PendingEntries;
pub(super) struct PendingEndpoints {
    owner: PendingEndpointMap,
    expected: Arc<Entries>,
    published: bool,
}
impl PendingEndpoints {
    pub(super) fn staged(
        owner: &PendingEndpointMap,
        config: &zero_config::RuntimeConfig,
    ) -> Option<Arc<PendingEntries>> {
        owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|entry| entry.candidate_matches(config))
            .cloned()
    }
    pub(super) fn stage(
        owner: &PendingEndpointMap,
        candidate: PendingEntries,
    ) -> Result<Arc<PendingEntries>, EngineError> {
        let mut current = owner.lock().unwrap_or_else(|e| e.into_inner());
        if current.is_some() {
            return Err(invalid(
                "previous WireGuard candidate still owns its transition",
            ));
        }
        let expected = Arc::new(candidate);
        *current = Some(expected.clone());
        Ok(expected)
    }
    pub(super) fn adopt(
        owner: PendingEndpointMap,
        expected: Arc<PendingEntries>,
    ) -> Result<Self, EngineError> {
        if !owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|entry| Arc::ptr_eq(entry, &expected))
        {
            return Err(invalid(
                "actual cold device candidate changed before BuildNew",
            ));
        }
        Ok(Self {
            owner,
            expected,
            published: false,
        })
    }
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
        entries: HashMap<String, Arc<PendingLinkedEndpoint>>,
    ) -> Result<Self, EngineError> {
        let expected = Arc::new(PendingEntries::new(entries));
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(super) fn release_closed(
        owner: &PendingEndpointMap,
        expected: &Arc<Entries>,
    ) -> Result<(), EngineError> {
        let mut current = owner.lock().unwrap_or_else(|error| error.into_inner());
        if !current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            return Err(invalid(
                "WireGuard cleanup belongs to another actual candidate",
            ));
        }
        for pending in expected.values() {
            pending.revoke();
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
        let mut current = self.owner.lock().unwrap_or_else(|error| error.into_inner());
        if !current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &self.expected))
        {
            return;
        }
        if !self.published {
            for pending in self.expected.values() {
                pending.revoke();
            }
        }
        if self.published
            || !self
                .expected
                .values()
                .any(|pending| pending.needs_cleanup())
        {
            *current = None;
        }
    }
}

#[cfg(test)]
mod tests;
