//! One actual candidate's native/endpoint/authentication setup.
use super::*;
impl Generation {
    pub(super) async fn initialize_candidate(
        &self,
        entry: &Entry,
        candidate: Candidate,
        end: Instant,
    ) -> Result<(), SetupFailure> {
        let mut state = entry.state.lock().await;
        let options = &self.profiles[candidate.profile];
        if state.candidates.is_none() {
            let mut resolve = candidate.call;
            resolve.deadline = None;
            resolve.cancel = entry.cancel.clone();
            let candidates = tokio::time::timeout_at(
                end,
                candidate
                    .upstream
                    .resolve_controlled_carrier(options.endpoint.clone(), resolve),
            )
            .await
            .map_err(|source| {
                SetupFailure::terminal(error(Stage::Dial, Reason::Timeout).with_source(source))
            })?
            .map_err(SetupFailure::terminal)?;
            if candidates.is_empty() {
                return Err(SetupFailure::terminal(error(Stage::Dial, Reason::Io)));
            }
            if candidate.upstream.carrier_dns_generation() != candidate.dns {
                return Err(SetupFailure::terminal(error(
                    Stage::Prepare,
                    Reason::StalePlan,
                )));
            }
            state.candidates = Some(candidates);
        }
        self.retain_failed_candidate(&mut state)
            .map_err(SetupFailure::terminal)?;
        let connection = self
            .open_candidate(
                &mut state,
                candidate.profile,
                candidate.index,
                &candidate.client,
                end,
            )
            .await
            .map_err(|source| SetupFailure {
                source: Arc::new(source),
                next_candidate: true,
            })?;
        state.selected_candidate = Some(candidate.index);
        state.pending = Some(
            OwnedAuthentication::prepare(connection, state.owner.as_ref().unwrap())
                .map_err(SetupFailure::terminal)?,
        );
        state.pending.as_mut().unwrap().udp_sessions = self.udp_sessions;
        state.pending.as_mut().unwrap().udp_pending_bytes = self.udp_pending_bytes;
        tokio::time::timeout_at(
            end,
            state.pending.as_mut().unwrap().authenticate(
                &options.leaf.password,
                options.leaf.settings,
                &self.tasks,
            ),
        )
        .await
        .map_err(|source| {
            SetupFailure::terminal(error(Stage::Handshake, Reason::Timeout).with_source(source))
        })?
        .map_err(SetupFailure::terminal)?;
        *state
            .pending
            .as_ref()
            .unwrap()
            .native_origin
            .lock()
            .unwrap() = state.origin.clone();
        state.pending.as_mut().unwrap().dns_generation = candidate.dns;
        let auth = Arc::new(state.pending.take().unwrap());
        state.udp_join = auth.udp_witness().await.map_err(SetupFailure::terminal)?;
        state.auth = Some(auth);
        Ok(())
    }
}
