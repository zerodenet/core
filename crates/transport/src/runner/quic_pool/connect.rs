//! Single-flight entry acquisition, network/DNS transition fencing, and
//! paired PacketSegment + OwnedEndpointOwner candidate dialing.

use super::*;

impl<P: QuicPoolProtocol> QuicPoolGeneration<P> {
    pub async fn acquire(
        self: &Arc<Self>,
        profile: usize,
        binding: UpstreamBinding,
        expected_dns: Option<Option<DnsGeneration>>,
        waiting: Option<PendingCarrierObserver>,
        initializer_cancel: CancellationToken,
        end: Instant,
    ) -> Result<Arc<P::Session>, ChannelError> {
        if !binding.prefix.is_empty() {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        self.profiles
            .get(profile)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let call = CarrierContext {
            session: None,
            deadline: Some(end),
            cancel: self.cancel.child_token(),
        };
        let upstream = match waiting.clone() {
            Some(waiting) => {
                self.upstreams[profile]
                    .select_background_carrier_observed(Some(binding), call.clone(), waiting)
                    .await?
            }
            None => {
                self.upstreams[profile]
                    .select_background_carrier(Some(binding), call.clone())
                    .await?
            }
        };
        let key = Key {
            profile,
            binding: upstream.binding().clone(),
            dns_generation: upstream.carrier_dns_generation(),
        };
        if expected_dns.is_some_and(|expected| expected != key.dns_generation) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        // Network generations cannot acquire replacement originals while old
        // endpoint users, workers or native receipts remain unsettled.
        let stale = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(old, _)| {
                old.binding.network_generation != key.binding.network_generation
                    || (old.profile == key.profile && old.dns_generation != key.dns_generation)
            })
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (old, entry) in stale {
            entry.close(end).await?;
            self.remove_entry(&old, &entry).await?;
        }
        let entry = self.admit_entry(&key, end).await?;
        self.trace_budget();
        *entry.touched.lock().unwrap() = Instant::now();
        let _initialization = tokio::time::timeout_at(end, entry.initialization.lock())
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?;
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?;
        if state.initializing {
            drop(state);
            entry.receive_setup(end).await?;
            entry.wait_setup_ack(end).await?;
            drop(_initialization);
            return Box::pin(self.acquire(
                profile,
                key.binding.clone(),
                expected_dns,
                waiting,
                initializer_cancel,
                end,
            ))
            .await;
        }
        if let Some(session) = &state.session {
            if P::is_session_ready(session)
                && !entry.cancel.is_cancelled()
                && !state.closed
                && state
                    .original_stop
                    .as_ref()
                    .is_none_or(|stop| !stop.is_cancelled())
                && state
                    .retirement
                    .as_ref()
                    .is_none_or(|original| !original.retirement_requested())
            {
                let session = session.clone();
                self.observe_retained_original(&state, profile, &upstream)?;
                drop(state);
                entry.wait_setup_ack(end).await?;
                return Ok(session);
            }
        }
        if state.attempted {
            drop(state);
            entry.close(end).await?;
            self.remove_entry(&key, &entry).await?;
            drop(_initialization);
            return Box::pin(self.acquire(
                profile,
                key.binding.clone(),
                expected_dns,
                waiting,
                initializer_cancel,
                end,
            ))
            .await;
        }
        state.attempted = true;
        state.tasks = Some(self.tasks.clone());
        drop(state);
        let client = self
            .clients
            .get()
            .and_then(|clients| clients.get(profile))
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .clone();
        let mut index = 0;
        loop {
            let result = self
                .setup_candidate(
                    &entry,
                    setup::Candidate {
                        profile,
                        index,
                        upstream: upstream.clone(),
                        call: call.clone(),
                        client: client.clone(),
                        dns: key.dns_generation,
                        initializer_cancel: initializer_cancel.clone(),
                    },
                    end,
                )
                .await;
            match result {
                Ok(()) => {
                    entry.wait_setup_ack(end).await?;
                    return entry
                        .state
                        .lock()
                        .await
                        .session
                        .clone()
                        .ok_or_else(|| error(Stage::Prepare, Reason::Internal));
                }
                Err(source) => {
                    let state = entry.state.lock().await;
                    let retry = state
                        .setup_failure
                        .as_ref()
                        .is_some_and(|failure| failure.next_candidate);
                    if !retry {
                        return Err(source);
                    }
                    drop(state);
                    let retirement = entry.retire_current_endpoint(end).await;
                    entry.receive_native_failure().await?;
                    retirement?;
                    let state = entry.state.lock().await;
                    let count = state
                        .candidates
                        .as_ref()
                        .map_or(0, |candidates| candidates.len());
                    drop(state);
                    entry.wait_endpoint_ack(end).await?;
                    entry.wait_setup_ack(end).await?;
                    index += 1;
                    if index >= count {
                        return Err(source);
                    }
                }
            }
        }
    }

    pub(super) async fn open_candidate(
        &self,
        state: &mut EntryState<P>,
        profile: usize,
        index: usize,
        client: &quinn::ClientConfig,
        end: Instant,
    ) -> Result<OwnedConnection, ChannelError> {
        let options = &self.profiles[profile];
        if state.permit.is_none() || state.retirement.is_none() {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let port = P::profile_udp_hop(options).map(|hop| hop.next_port());
        let mut logical = state
            .candidates
            .as_ref()
            .unwrap()
            .peer(index)
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        logical.set_port(P::profile_port(options));
        let (socket, queues) = PacketSocket::prepare_with_transport_options(
            logical,
            self.buffers.clone(),
            &P::profile_masks(options),
        )
        .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let (peer, grant) = state.candidates.as_ref().unwrap().open_metadata(
            index,
            port,
            state.permit.as_ref().unwrap(),
            state.caller.as_ref().unwrap(),
            end,
        )?;
        state.unbound = Some(grant);
        state.logical = Some(logical);
        state.next_hop =
            P::profile_udp_hop(options).map(|hop| Instant::now() + hop.next_interval());
        let limits = OwnedEndpointLimits {
            drivers: n(3),
            users: self.limits.endpoint_users,
        };
        let segment = match PacketSegment::prepare_with_endpoint(
            state.unbound.take().unwrap(),
            queues,
            self.tasks.clone(),
            peer,
            logical,
            limits,
        ) {
            Ok(segment) => segment,
            Err(rejected) => {
                let rejected = *rejected;
                state.unbound = Some(rejected.grant);
                state.queues = Some(rejected.queues);
                return Err(error(Stage::Prepare, Reason::Io).with_source(rejected.error));
            }
        };
        state.segment = Some(segment);
        let (owner, receiver) = OwnedEndpointOwner::new_packet_endpoint(
            socket,
            state.segment.as_mut().unwrap(),
            self.tasks.clone(),
            limits,
        )?;
        state.owner = Some(owner);
        state.receiver = Some(receiver);
        state.segment.as_mut().unwrap().start()?;
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), None, Some(client.clone()))?;
        if let Some(retirement) = &state.retirement {
            let receiver = state
                .owner
                .as_ref()
                .unwrap()
                .prepare_native_endpoint_receiver(
                    state.segment.as_ref().unwrap(),
                    state.origin.as_ref().unwrap(),
                    retirement,
                )?;
            receiver.confirm()?;
        }
        tokio::time::timeout_at(
            end,
            handle.connect(logical, P::profile_server_name(options)),
        )
        .await
        .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))?
    }
}
