//! One original response Set per real peer; never retained by its join observer.
use super::*;
use crate::runtime::udp_flow::response_join::{ResponseSetClosed, ResponseSetOwner};

pub(super) struct ResponseOwner {
    owner: Arc<ResponseSetOwner>,
    closed: Option<ResponseSetClosed>,
    failure: Option<SharedError>,
}
impl HostPeerBinding {
    /// Install the actual dispatcher's original Set before its first response
    /// spawn. This is not another child-resource claim or a caller-join proof.
    pub(crate) fn install_response_owner(
        &self,
        owner: &Arc<ResponseSetOwner>,
    ) -> Result<(), ChannelError> {
        // The real Scope's joined publication uses this same short lock, so a
        // response Set cannot attach after its containing peer was reclaimed.
        let state = self.join.state.lock().expect("peer binding poisoned");
        if state.task.is_none() || state.joined.is_some() || state.rejected.is_some() {
            return Err(self.error(
                Reason::Revoked,
                "response owner requires a live registered peer",
            ));
        }
        let mut responses = self.responses.lock().expect("peer responses poisoned");
        if let Some(actual) = responses.as_ref() {
            return if Arc::ptr_eq(&actual.owner, owner) {
                Ok(())
            } else {
                Err(self.error(Reason::Capacity, "one original response Set per peer"))
            };
        }
        *responses = Some(ResponseOwner {
            owner: owner.clone(),
            closed: None,
            failure: None,
        });
        Ok(())
    }
    pub(in crate::runtime::inbound_owners) fn responses_pending(&self) -> bool {
        self.responses
            .lock()
            .expect("peer responses poisoned")
            .as_ref()
            .is_some_and(|actual| actual.closed.is_none())
    }
    /// Only called after the containing peer's actual Scope receipt. Its sole
    /// dispatcher lease must return before the original Set can really join.
    pub(in crate::runtime::inbound_owners) async fn close_responses(
        &self,
        deadline: Instant,
    ) -> Result<Option<SharedError>, ChannelError> {
        let owner = {
            let responses = self.responses.lock().expect("peer responses poisoned");
            let Some(actual) = responses.as_ref() else {
                return Ok(None);
            };
            if actual.closed.is_some() {
                return Ok(actual.failure.clone());
            }
            actual.owner.clone()
        };
        owner.revoke();
        let closed = owner.shutdown(deadline).await?;
        assert!(closed.matches(&owner), "original response Set receipt");
        if let Some(crate::runtime::udp_flow::response_join::ResponseFailure::Body(source)) =
            owner.first_failure()
        {
            tracing::debug!(error = %source, "peer response body failure retained after actual join");
        }
        tracing::trace!(
            response_tasks = closed.joined_tasks(),
            "original peer response Set joined"
        );
        // Physical response-task completion and an original fatal source are
        // independent. Persist both before another await/native cleanup.
        let fatal = owner.first_fatal_failure().map(|source| {
            let mut error = ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                .with_source(source);
            error.component = Some(self.parent.component);
            Arc::new(error)
        });
        let mut responses = self.responses.lock().expect("peer responses poisoned");
        let actual = responses.as_mut().expect("installed original response Set");
        assert!(Arc::ptr_eq(&actual.owner, &owner));
        actual.closed.get_or_insert(closed);
        if actual.failure.is_none() {
            actual.failure = fatal;
        }
        Ok(actual.failure.clone())
    }
}
