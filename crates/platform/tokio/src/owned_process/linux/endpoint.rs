//! Same cold process original's endpoint and actor association.
use super::*;
impl OwnedProcess {
    /// The witness supplies join custody, never executable/native permission.
    pub fn bind_actor(&self, actor: TaskJoinWitness) -> io::Result<()> {
        if !self.scope.same_scope(&actor.scope_identity()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actor is not in the original process Scope",
            ));
        }
        let mut state = self.lock();
        if state.actor.is_some()
            || state.start_attempted
            || state.stop
            || !state.originals.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "actual process actor is already bound or stopped",
            ));
        }
        state.actor = Some(actor);
        Ok(())
    }
    pub(super) fn endpoint_is_running(&self) -> bool {
        let state = self.lock();
        state.started
            && !state.stop
            && state.root_status.is_none()
            && state.helper_status.is_none()
            && state.first_error.is_none()
    }
    pub fn attach_endpoint(
        self: &Arc<Self>,
        endpoint: &Arc<super::endpoint::ProcessEndpointHandoff>,
    ) -> io::Result<()> {
        let mut state = self.lock();
        if state.start_attempted
            || state.stop
            || state.endpoint.is_some()
            || !state.originals.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process is not cold",
            ));
        }
        // Persistent before the association can escape, including failed bind.
        state.endpoint = Some(endpoint.clone());
        drop(state);
        endpoint.attach(self, &self.scope)
    }
    /// Host completes the same already-budgeted cold endpoint job. The program
    /// must remain the originally declared executable; no native work occurs.
    pub fn install_endpoint_request(
        &self,
        request: ProcessRequest,
        endpoint: &Arc<super::endpoint::ProcessEndpointHandoff>,
    ) -> io::Result<()> {
        let mut state = self.lock();
        if state.start_attempted
            || state.stop
            || state.endpoint_request_ready
            || !state
                .endpoint
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, endpoint))
            || !state
                .request
                .as_ref()
                .is_some_and(|cold| cold.program == request.program)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "endpoint job request is not the same cold declaration",
            ));
        }
        state.request = Some(request);
        state.endpoint_request_ready = true;
        Ok(())
    }
    pub(super) fn endpoint(&self) -> Option<Arc<super::endpoint::ProcessEndpointHandoff>> {
        self.lock().endpoint.clone()
    }
}
