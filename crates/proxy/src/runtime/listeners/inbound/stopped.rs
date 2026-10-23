//! Immutable facts from this actual listener StopOld barrier, never a grant.
use crate::runtime::inbound_owners::InboundTerminal;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};
use zero_config::RuntimeConfig;

/// Delivered only after every affected original listener has confirmed shutdown.
/// Not Clone: the before-bind host phase receives it once. This contains neither
/// outbound-user/task/native completion nor permission to start a device.
pub(in crate::runtime) struct StoppedInbounds {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    candidate: Weak<RuntimeConfig>,
    closed: BTreeMap<String, Arc<InboundTerminal>>,
    absent: BTreeSet<String>,
}
impl StoppedInbounds {
    pub(super) fn new(candidate: Weak<RuntimeConfig>) -> Self {
        Self {
            candidate,
            closed: BTreeMap::new(),
            absent: BTreeSet::new(),
        }
    }
    pub(super) fn record_terminal(&mut self, tag: String, terminal: Arc<InboundTerminal>) {
        self.closed.insert(tag, terminal);
    }
    pub(super) fn record_absent(&mut self, tag: String) {
        self.absent.insert(tag);
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(in crate::runtime) fn matches_candidate(&self, config: &Arc<RuntimeConfig>) -> bool {
        self.candidate.ptr_eq(&Arc::downgrade(config))
    }
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(in crate::runtime) fn terminal(&self, tag: &str) -> Option<&InboundTerminal> {
        self.closed.get(tag).map(Arc::as_ref)
    }
    /// This is only observed registry absence after reservation cleanup, not
    /// historical physical-close proof. A retained Unknown owner is never absent.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit cold-device library path; production activation requires the owning replacement transaction"
        )
    )]
    pub(in crate::runtime) fn owner_was_absent(&self, tag: &str) -> bool {
        self.absent.contains(tag)
    }
}
