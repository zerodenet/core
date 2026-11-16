use super::*;
impl Generation {
    pub(super) async fn acquire(
        self: &Arc<Self>,
        profile: usize,
        binding: UpstreamBinding,
        expected_dns: Option<Option<zero_platform_tokio::contract::dns::DnsGeneration>>,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
        initializer_cancel: zero_platform_tokio::contract::CancellationToken,
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
                    resources: self.resources.clone(),
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
        if let Some(auth) = &state.auth {
            if auth.is_ready()
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
                let auth = auth.clone();
                self.observe_retained_original(&state, profile, &upstream)?;
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
                        .auth
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
                    // Native/users closure is independent of the failed setup's
                    // Host acknowledgement. Publish both before fresh admission.
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
}

mod native;
