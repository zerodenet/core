//! Positive caller-native facts are separate from retained typed task failures.
use super::*;
impl custody::NativeUse {
    fn native_retirement_complete(&self) -> bool {
        let logical_complete = {
            let Ok(logical) = self.logical.try_lock() else {
                return false;
            };
            logical
                .as_ref()
                .is_none_or(|owner| owner.closed().is_some_and(|closed| closed.matches(owner)))
        };
        logical_complete
            && self
                .original
                .closed()
                .is_some_and(|closed| closed.matches(&self.original))
            && self
                .original
                .users_closed()
                .is_some_and(|closed| closed.matches(&self.original))
    }
}
impl Provider {
    pub(super) fn caller_native_retirement_complete(&self, caller: &TaskJoinWitness) -> bool {
        if !caller
            .scope_identity()
            .same_scope(&self.registrar.scope_identity())
        {
            return false;
        }
        let users = self
            .users
            .lock()
            .expect("DNS users poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        let pending = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(caller))
            .cloned()
            .collect::<Vec<_>>();
        if !users
            .iter()
            .chain(&pending)
            .all(|entry| entry.native_retirement_complete())
        {
            return false;
        }
        #[cfg(feature = "dns")]
        {
            let quic = self
                .quic_users
                .lock()
                .expect("DNS QUIC users poisoned")
                .iter()
                .filter(|entry| entry.task.same_registration(caller))
                .cloned()
                .collect::<Vec<_>>();
            if !quic.iter().all(|entry| entry.native_retirement_complete()) {
                return false;
            }
        }
        true
    }
}

/// The actual lifecycle receiver retains every original worker/cleanup Arc
/// after all physical cleanup has run, including secondary ownership faults.
#[derive(Debug)]
struct FaultHandoff {
    original: Option<BuildError>,
    faults: Arc<zero_dns::OwnedDnsQueryFaults>,
}
impl std::fmt::Display for FaultHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.faults.as_ref(), f)
    }
}
impl std::error::Error for FaultHandoff {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.original {
            Some(original) => Some(original),
            None => Some(self.faults.as_ref()),
        }
    }
}
pub(in crate::component::native_dns) fn handoff_faults(
    original: Option<BuildError>,
    faults: Arc<zero_dns::OwnedDnsQueryFaults>,
) -> BuildError {
    error(std::io::Error::other(FaultHandoff { original, faults }))
}
