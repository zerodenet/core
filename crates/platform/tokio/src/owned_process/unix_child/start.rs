use super::*;
impl OwnedProcess {
    /// The actual original Child is installed synchronously before readiness
    /// can suspend. Build, a foreign Scope and a second start cannot spawn.
    pub fn start(&self, actor: TaskJoinWitness) -> io::Result<()> {
        if !self.scope.resource_open()
            || !self.scope.same_scope(&actor.scope_identity())
            || actor.is_joined()
        {
            return Err(denied("process needs its actual live original actor"));
        }
        if let Some(endpoint) = self.endpoint() {
            endpoint.check_start(&actor, self)?;
        }
        let mut state = self.lock();
        if state.start_attempted
            || state.stop
            || (state.endpoint.is_some() && !state.endpoint_request_ready)
        {
            return Err(denied("native child start is terminal"));
        }
        if let Some(actual) = &state.actor {
            if !actual.same_registration(&actor) {
                return Err(denied("process start crossed its bound original actor"));
            }
        } else {
            state.actor = Some(actor);
        }
        state.start_attempted = true;
        let result = state
            .request
            .as_ref()
            .ok_or_else(|| denied("original process request unavailable"))?
            .command()
            .spawn();
        match result {
            Ok(child) => {
                state.child = Some(child);
                state.acquired = true;
                state.request = None;
                Ok(())
            }
            Err(source) => {
                state.fault(source);
                state.stop = true;
                state.request = None;
                Err(original_failure(state.first_error.as_ref().unwrap()))
            }
        }
    }
}
