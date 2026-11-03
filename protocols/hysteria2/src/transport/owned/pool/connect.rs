use super::*;
impl Generation {
    pub(super) async fn acquire(
        &self,
        profile: usize,
        binding: UpstreamBinding,
        end: Instant,
    ) -> Result<Arc<OwnedAuthentication>, ChannelError> {
        if !binding.prefix.is_empty() {
            return Err(error(Stage::Prepare, Reason::Unsupported));
        }
        let options = self
            .profiles
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
        };
        // Network generations cannot acquire replacement originals while old
        // endpoint users, workers or native receipts remain unsettled.
        let stale = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(old, _)| old.binding.network_generation != key.binding.network_generation)
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (old, entry) in stale {
            entry.close(end).await?;
            self.remove_entry(&old, &entry);
        }
        let pressured = {
            let entries = self.entries.lock().unwrap();
            entries.len() >= 256 && !entries.contains_key(&key)
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
                let idle = entry
                    .state
                    .try_lock()
                    .ok()
                    .is_some_and(|state| state.idle());
                if idle {
                    entry.close(end).await?;
                    self.remove_entry(&old, &entry);
                    break;
                }
            }
        }
        let entry = {
            let mut entries = self.entries.lock().unwrap();
            if let Some(entry) = entries.get(&key) {
                entry.clone()
            } else {
                if entries.len() >= 256 {
                    return Err(error(Stage::Prepare, Reason::Capacity));
                }
                let entry = Arc::new(Entry {
                    cancel: self.cancel.child_token(),
                    state: Default::default(),
                    touched: Mutex::new(Instant::now()),
                });
                entries.insert(key.clone(), entry.clone());
                entry
            }
        };
        *entry.touched.lock().unwrap() = Instant::now();
        let mut state = entry.state.lock().await;
        if let Some(auth) = &state.auth {
            if auth.is_ready() {
                return Ok(auth.clone());
            }
        }
        if state.attempted {
            drop(state);
            entry.close(end).await?;
            self.remove_entry(&key, &entry);
            return Box::pin(self.acquire(profile, key.binding.clone(), end)).await;
        }
        state.attempted = true;
        state.tasks = Some(self.tasks.clone());
        let stop = entry.cancel.clone();
        let (_, caller) = self
            .tasks
            .spawn_tracked(TaskClass::Auxiliary, async move {
                stop.cancelled().await;
                Ok(())
            })
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        state.caller = Some(caller);
        let client = self
            .clients
            .get()
            .and_then(|clients| clients.get(profile))
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?
            .clone();
        let mut resolve = call.clone();
        resolve.deadline = None;
        resolve.cancel = entry.cancel.clone();
        let candidates = tokio::time::timeout_at(
            end,
            upstream.resolve_controlled_carrier(options.endpoint.clone(), resolve),
        )
        .await
        .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))??;
        if candidates.is_empty() {
            return Err(error(Stage::Dial, Reason::Io));
        }
        state.candidates = Some(candidates);
        let count = state.candidates.as_ref().unwrap().len();
        let mut connected = None;
        for index in 0..count {
            match self
                .open_candidate(&mut state, profile, index, &client, end)
                .await
            {
                Ok(connection) => {
                    state.selected_candidate = Some(index);
                    connected = Some(connection);
                    break;
                }
                Err(source) => {
                    state.candidate_failures.push(Arc::new(source));
                    state.retire_endpoint(end).await?;
                }
            }
        }
        let endpoint = connected.ok_or_else(|| {
            error(Stage::Dial, Reason::Io).with_source(OriginalChannel(
                state
                    .candidate_failures
                    .last()
                    .expect("nonempty candidate set")
                    .clone(),
            ))
        })?;
        state.pending = Some(OwnedAuthentication::prepare(
            endpoint,
            state.owner.as_ref().unwrap(),
        )?);
        state
            .pending
            .as_mut()
            .unwrap()
            .authenticate(&options.leaf.password, options.leaf.settings, &self.tasks)
            .await?;
        state.pending.as_mut().unwrap().native_origin = state.origin.clone();
        let auth = Arc::new(state.pending.take().unwrap());
        state.udp_join = auth.udp_witness();
        state.baseline = state.owner.as_ref().unwrap().active_users();
        state.auth = Some(auth.clone());
        Ok(auth)
    }
}
impl Generation {
    pub(super) fn remove_entry(&self, key: &Key, entry: &Arc<Entry>) {
        let mut entries = self.entries.lock().unwrap();
        if entries
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(key);
        }
    }
    async fn open_candidate(
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
        let (peer, grant) = state.candidates.as_ref().unwrap().open_metadata(
            index,
            port,
            state.permit.as_ref().unwrap(),
            state.caller.as_ref().unwrap(),
            end,
        )?;
        state.unbound = Some(grant);
        let mut logical = peer;
        logical.set_port(options.leaf.port);
        state.logical = Some(logical);
        state.next_hop = options
            .leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| Instant::now() + hop.next_interval());
        let (socket, queues) = PacketSocket::prepare(logical)
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
        let socket = socket
            .with_stateless_masks(&options.masks())
            .map_err(|source| error(Stage::Prepare, Reason::Io).with_source(source))?;
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
        state.segment.as_mut().unwrap().start()?;
        let (owner, receiver) = OwnedEndpointOwner::new_packet(
            socket,
            self.tasks.clone(),
            OwnedEndpointLimits {
                drivers: n(3),
                users: n(65536),
            },
        );
        state.owner = Some(owner);
        state.receiver = Some(receiver);
        let handle =
            state
                .owner
                .as_mut()
                .unwrap()
                .start(Default::default(), None, Some(client.clone()))?;
        handle
            .connect(
                logical,
                options
                    .leaf
                    .server_name
                    .as_deref()
                    .unwrap_or(&options.leaf.server),
            )
            .await
    }
}
