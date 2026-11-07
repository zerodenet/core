use super::*;
impl Generation {
    pub(super) async fn acquire(
        self: &Arc<Self>,
        profile: usize,
        binding: UpstreamBinding,
        expected_dns: Option<Option<zero_platform_tokio::contract::dns::DnsGeneration>>,
        end: Instant,
    ) -> Result<Arc<OwnedAuthentication>, ChannelError> {
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
        let upstream = self.upstreams[profile]
            .select_background_carrier(Some(binding), call.clone())
            .await?;
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
        let pressured = {
            let entries = self.entries.lock().unwrap();
            entries.len() + self.failures.lock().unwrap().len() >= self.entry_limit.get()
                && !entries.contains_key(&key)
        };
        if pressured {
            let mut candidates = self
                .entries
                .lock()
                .unwrap()
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone(), *entry.touched.lock().unwrap()))
                .collect::<Vec<_>>();
            candidates.sort_by_key(|(_, _, touched)| *touched);
            for (old, entry, _) in candidates {
                if entry.close_if_idle(end).await? {
                    self.remove_entry(&old, &entry).await?;
                    break;
                }
            }
        }
        let entry = {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get(&key) {
                entry.clone()
            } else {
                if entries.len() + self.failures.lock().unwrap().len() >= self.entry_limit.get() {
                    return Err(error(Stage::Prepare, Reason::Capacity));
                }
                let entry = Arc::new(Entry {
                    initialization: Default::default(),
                    cancel: self.cancel.child_token(),
                    state: Default::default(),
                    touched: Mutex::new(Instant::now()),
                });
                entries.insert(key.clone(), entry.clone());
                entry
            }
        };
        self.trace_budget();
        *entry.touched.lock().unwrap() = Instant::now();
        // Preserve the old one-initializer semantics across genuine sequential
        // task/ACK boundaries; no second borrower retires a candidate between
        // the first initializer's failed native close and its next reservation.
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
            return Box::pin(self.acquire(profile, key.binding.clone(), expected_dns, end)).await;
        }
        if let Some(auth) = &state.auth {
            if auth.is_ready() && !entry.cancel.is_cancelled() && !state.closed {
                let auth = auth.clone();
                drop(state);
                entry.wait_setup_ack(end).await?;
                return Ok(auth);
            }
        }
        if state.attempted {
            drop(state);
            entry.close(end).await?;
            self.remove_entry(&key, &entry).await?;
            drop(_initialization);
            return Box::pin(self.acquire(profile, key.binding.clone(), expected_dns, end)).await;
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
                        .auth
                        .clone()
                        .ok_or_else(|| error(Stage::Prepare, Reason::Internal));
                }
                Err(source) => {
                    let mut state = entry.state.lock().await;
                    let retry = state
                        .setup_failure
                        .as_ref()
                        .is_some_and(|failure| failure.next_candidate);
                    if !retry {
                        return Err(source);
                    }
                    // Native/users closure is independent of the failed setup's
                    // Host acknowledgement. Publish both before fresh admission.
                    state.retire_endpoint(end).await?;
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
}
impl Generation {
    pub(super) async fn open_candidate(
        &self,
        state: &mut EntryState,
        profile: usize,
        index: usize,
        client: &quinn::ClientConfig,
        end: Instant,
    ) -> Result<OwnedConnection, ChannelError> {
        let options = &self.profiles[profile];
        let permit = self.metadata[profile]
            .grants()
            .reserve(state.caller.as_ref().unwrap())?;
        state.origin = Some(permit.origin());
        state.permit = Some(permit);
        let port = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| hop.next_port());
        let mut logical = state
            .candidates
            .as_ref()
            .unwrap()
            .peer(index)
            .ok_or_else(|| error(Stage::Prepare, Reason::InvalidConfig))?;
        logical.set_port(options.leaf.port);
        // The original N is already held. Prepay actual scratch/codec/copy
        // before opening any native candidate; no second byte book is created.
        let (socket, queues) =
            PacketSocket::prepare_with_budget(logical, self.buffers.clone(), &options.masks())
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
        state.next_hop = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| Instant::now() + hop.next_interval());
        let segment = match PacketSegment::prepare(
            state.unbound.take().unwrap(),
            queues,
            self.tasks.clone(),
            peer,
            logical,
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
        let (owner, receiver) = OwnedEndpointOwner::new_packet(
            socket,
            self.tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(3),
                users: self.endpoint_users,
            },
        );
        state.owner = Some(owner);
        state.receiver = Some(receiver);
        state
            .owner
            .as_ref()
            .unwrap()
            .bind_packet_segment(state.segment.as_ref().unwrap())?;
        state.segment.as_mut().unwrap().start()?;
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), None, Some(client.clone()))?;
        tokio::time::timeout_at(
            end,
            handle.connect(
                logical,
                options
                    .leaf
                    .server_name
                    .as_deref()
                    .unwrap_or(&options.leaf.server),
            ),
        )
        .await
        .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))?
    }
}
