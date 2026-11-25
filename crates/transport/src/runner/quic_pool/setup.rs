//! Genuine sequential initialization on this pool's original Scope and Entry.

use super::*;

#[derive(Clone)]
pub(super) struct SetupFailure {
    pub(super) source: Arc<ChannelError>,
    pub(super) next_candidate: bool,
}

impl SetupFailure {
    fn terminal(source: ChannelError) -> Self {
        Self {
            source: Arc::new(source),
            next_candidate: false,
        }
    }
}

pub(super) struct Candidate {
    pub(super) profile: usize,
    pub(super) index: usize,
    pub(super) upstream: UpstreamTransport,
    pub(super) call: CarrierContext,
    pub(super) client: quinn::ClientConfig,
    pub(super) dns: Option<DnsGeneration>,
    pub(super) initializer_cancel: CancellationToken,
}

enum Setup {
    Candidate(Candidate),
    Hop(usize),
}

impl<P: QuicPoolProtocol> QuicPoolGeneration<P> {
    pub(super) async fn setup_candidate(
        self: &Arc<Self>,
        entry: &Arc<Entry<P>>,
        candidate: Candidate,
        end: Instant,
    ) -> Result<(), ChannelError> {
        self.run_setup(entry, Setup::Candidate(candidate), end)
            .await
    }

    pub(super) async fn setup_hop(
        self: &Arc<Self>,
        entry: &Arc<Entry<P>>,
        profile: usize,
        end: Instant,
    ) -> Result<bool, ChannelError> {
        self.run_setup(entry, Setup::Hop(profile), end).await?;
        entry.wait_setup_ack(end).await?;
        Ok(entry.state.lock().await.setup_worked)
    }

    async fn run_setup(
        self: &Arc<Self>,
        entry: &Arc<Entry<P>>,
        operation: Setup,
        end: Instant,
    ) -> Result<(), ChannelError> {
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?;
        if state.initializing || entry.cancel.is_cancelled() || state.closed {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        if Instant::now() >= end {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        if state.caller.as_ref().is_some_and(|caller| {
            caller
                .try_joined()
                .is_none_or(|joined| !joined.registration_released())
        }) {
            return Err(error(Stage::Prepare, Reason::Capacity));
        }
        let callbacks = self.tasks.defer_callbacks();
        let preparation = match &operation {
            Setup::Candidate(candidate) => {
                match self.prepare_original(entry, &mut state, candidate) {
                    Ok(preparation) => preparation,
                    Err(source) => {
                        let failure = SetupFailure::terminal(source);
                        let source = failure.source.clone();
                        state.setup_failure = Some(failure);
                        drop(state);
                        drop(callbacks);
                        return Err(ChannelError::retained(source));
                    }
                }
            }
            Setup::Hop(_) => None,
        };
        state.initializing = true;
        state.setup_registration = Some(self.tasks.retirement_observation());
        state.setup_failure = None;
        state.setup_worked = false;
        let (admit, admitted) = tokio::sync::oneshot::channel();
        let actual = entry.clone();
        let generation = self.clone();
        let original_stop = state
            .original_stop
            .clone()
            .unwrap_or_else(|| entry.cancel.clone());
        let initializer_cancel = match &operation {
            Setup::Candidate(candidate) => candidate.initializer_cancel.clone(),
            Setup::Hop(_) => entry.cancel.clone(),
        };
        let work = async move {
            if admitted.await.is_err() {
                return Ok(());
            }
            let work = async {
                match operation {
                    Setup::Candidate(candidate) => generation
                        .initialize_candidate(&actual, candidate, end)
                        .await
                        .map(|()| true),
                    Setup::Hop(profile) => generation
                        .rotate(profile, &actual, end)
                        .await
                        .map_err(SetupFailure::terminal),
                }
            };
            let result = tokio::select! {
                biased;
                result = tokio::time::timeout_at(end, work) => Some(match result {
                    Ok(result) => result,
                    Err(source) => Err(SetupFailure::terminal(
                        error(Stage::Prepare, Reason::Timeout).with_source(source),
                    )),
                }),
                _ = actual.cancel.cancelled() => None,
                _ = original_stop.cancelled() => None,
                _ = initializer_cancel.cancelled() => None,
            };
            let mut state = actual.state.lock().await;
            state.initializing = false;
            match result {
                Some(Ok(worked)) => {
                    state.setup_worked = worked;
                    Ok(())
                }
                Some(Err(failure)) => {
                    if failure.next_candidate {
                        state.candidate_failures.push(failure.source.clone());
                    }
                    let source = failure.source.clone();
                    state.setup_failure = Some(failure);
                    Err(ChannelError::retained(source))
                }
                None => {
                    let reason = if initializer_cancel.is_cancelled() {
                        Reason::Cancelled
                    } else {
                        Reason::Revoked
                    };
                    state.setup_failure =
                        Some(SetupFailure::terminal(error(Stage::Prepare, reason)));
                    Ok(())
                }
            }
        };
        let registered = match preparation {
            Some(preparation) => preparation
                .spawn_auxiliary(&self.tasks, work)
                .map(|(permit, caller)| (caller, Some(permit))),
            None => self
                .tasks
                .spawn_tracked(TaskClass::Auxiliary, work)
                .map(|(_, caller)| (caller, None))
                .map_err(|source| {
                    error(
                        Stage::Prepare,
                        match source {
                            zero_platform_tokio::contract::scope::SpawnError::Closing => {
                                Reason::Revoked
                            }
                            zero_platform_tokio::contract::scope::SpawnError::NotStarted => {
                                Reason::InvalidConfig
                            }
                            _ => Reason::Capacity,
                        },
                    )
                    .with_source(source)
                }),
        };
        match registered {
            Ok((caller, permit)) => {
                state.setup_registration = None;
                state.caller = Some(caller);
                if let Some(permit) = permit {
                    state.permit = Some(permit);
                }
            }
            Err(source) => {
                state.setup_registration = None;
                state.initializing = false;
                let failure = SetupFailure::terminal(source);
                let source = failure.source.clone();
                state.setup_failure = Some(failure);
                drop(state);
                drop(callbacks);
                return Err(ChannelError::retained(source));
            }
        }
        drop(state);
        let _ = admit.send(());
        drop(callbacks);
        entry.receive_setup(end).await?;
        let state = entry.state.lock().await;
        state.setup_failure.as_ref().map_or(Ok(()), |failure| {
            Err(ChannelError::retained(failure.source.clone()))
        })
    }

    async fn initialize_candidate(
        &self,
        entry: &Entry<P>,
        candidate: Candidate,
        end: Instant,
    ) -> Result<(), SetupFailure> {
        let mut state = entry.state.lock().await;
        let profile = &self.profiles[candidate.profile];
        if state.candidates.is_none() {
            let mut resolve = candidate.call;
            resolve.deadline = None;
            resolve.cancel = entry.cancel.clone();
            let candidates = tokio::time::timeout_at(
                end,
                candidate
                    .upstream
                    .resolve_controlled_carrier(P::profile_endpoint(profile).clone(), resolve),
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
            P::prepare_session(
                connection,
                state.owner.as_ref().unwrap(),
                profile,
                &self.limits,
            )
            .map_err(SetupFailure::terminal)?,
        );
        tokio::time::timeout_at(
            end,
            P::authenticate(state.pending.as_mut().unwrap(), profile, &self.tasks),
        )
        .await
        .map_err(|source| {
            SetupFailure::terminal(error(Stage::Handshake, Reason::Timeout).with_source(source))
        })?
        .map_err(SetupFailure::terminal)?;
        let pending = state.pending.take().unwrap();
        let origin = state.origin.clone();
        let (session, witness) = P::finalize_session(pending, origin, candidate.dns)
            .await
            .map_err(SetupFailure::terminal)?;
        state.aux_join = witness;
        state.session = Some(session);
        Ok(())
    }
}
