//! A host-owned, single-attempt ticket from the actual Engine ID allocator.
use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Weak,
};
use zero_core::component::SessionId;

/// A minted identity is not an admission. The host keeps this sealed ticket
/// while passing its immutable id into protocol request facts. It owns no
/// registry entry, quota, task, Engine or runtime and has no public constructor.
/// Dropping an unadmitted ticket needs no deregistration or permanent history.
#[derive(Debug)]
pub struct SessionIdentity {
    id: SessionId,
    issuer: Weak<AtomicU64>,
    claimed: AtomicBool,
}
impl SessionIdentity {
    pub fn id(&self) -> SessionId {
        self.id
    }
    fn claim(&self, allocator: &Arc<AtomicU64>, id: SessionId) -> Result<(), EngineError> {
        if !Weak::ptr_eq(&self.issuer, &Arc::downgrade(allocator)) {
            return Err(denied(
                "session identity belongs to a different Engine allocator",
            ));
        }
        if self.id != id {
            return Err(denied("session identity does not match the request id"));
        }
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| denied("session identity was already used for an admission attempt"))?;
        Ok(())
    }
}
fn denied(reason: &str) -> EngineError {
    EngineError::AdmissionDenied {
        reason: reason.to_owned(),
    }
}

impl Engine {
    /// Allocate for a recognized business request without registering a flow.
    /// Engine clones share the same issuer; an unrelated Engine never does.
    pub fn mint_session_identity(&self) -> Result<SessionIdentity, EngineError> {
        Ok(SessionIdentity {
            id: self.allocate_session_id()?,
            issuer: Arc::downgrade(&self.next_session_id),
            claimed: AtomicBool::new(false),
        })
    }

    /// Admit the matching host-minted identity exactly once. Foreign tickets
    /// and mismatching Session ids are rejected before consuming the ticket or
    /// invoking policy. The first matching attempt consumes it even if policy
    /// refuses admission; it cannot later impersonate an active or settled id.
    pub fn prepare_session_with_identity(
        &self,
        identity: &SessionIdentity,
        session: &mut Session,
        inbound_tag: &str,
    ) -> Result<(), EngineError> {
        identity.claim(&self.next_session_id, SessionId(session.id))?;
        self.admit_session(session, inbound_tag)
    }

    pub(super) fn allocate_session_id(&self) -> Result<SessionId, EngineError> {
        let mut current = self.next_session_id.load(Ordering::Relaxed);
        loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| denied("Engine session identity space exhausted"))?;
            match self.next_session_id.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(SessionId(current)),
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests;
