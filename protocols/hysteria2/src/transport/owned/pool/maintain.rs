use super::*;
impl Generation {
    pub(super) async fn maintain(self: &Arc<Self>, end: Instant) -> Result<bool, ChannelError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        entries.sort_by_key(|(key, _)| key.profile);
        let cursor = *self.maintenance_cursor.lock().unwrap();
        let pivot = entries.partition_point(|(key, _)| key.profile < cursor);
        entries.rotate_left(pivot);
        for (key, entry) in entries {
            let Ok(_initialization) = entry.initialization.try_lock() else {
                continue;
            };
            let Ok(state) = entry.state.try_lock() else {
                continue;
            };
            if state.initializing {
                continue;
            }
            let idle = state.idle();
            let expired = entry.touched.lock().unwrap().elapsed()
                >= Duration::from_secs(
                    self.profiles[key.profile]
                        .leaf
                        .settings
                        .quic
                        .max_idle_timeout_secs,
                );
            let failed = state.attempted && state.auth.is_none();
            let hop = state.next_hop.is_some_and(|time| Instant::now() >= time);
            drop(state);
            let worked = if failed {
                entry.close(end).await?;
                self.remove_entry(&key, &entry).await?;
                true
            } else if idle && expired {
                if entry.close_if_idle(end).await? {
                    self.remove_entry(&key, &entry).await?;
                    true
                } else {
                    false
                }
            } else if hop {
                self.setup_hop(&entry, key.profile, end).await?
            } else {
                false
            };
            if worked {
                // One actual original per five-second slice. A successful
                // slice resumes from the next profile, including busy pools.
                *self.maintenance_cursor.lock().unwrap() = if key.profile + 1 == self.profiles.len()
                {
                    0
                } else {
                    key.profile + 1
                };
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(super) async fn rotate(
        &self,
        profile: usize,
        entry: &Entry,
        end: Instant,
    ) -> Result<bool, ChannelError> {
        let options = &self.profiles[profile];
        let Some(hop) = &options.leaf.node.udp_hop else {
            return Ok(false);
        };
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if entry.cancel.is_cancelled() || state.closed {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let selected = state
            .selected_candidate
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if state
            .candidates
            .as_ref()
            .and_then(|candidates| candidates.peer(selected))
            .is_none()
        {
            // Sealed DNS/network authority changed. Preserve this original;
            // only a fresh acquire may retire all old users/drivers/native and
            // then resolve/acquire a replacement with its actual new key.
            return Ok(false);
        }
        if state.receipt.is_none() {
            let segment = state
                .segment
                .as_mut()
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            let (queues, receipt) = segment.retire(end).await?;
            state.queues = Some(queues);
            state.receipt = Some(receipt);
        }
        // This is the original provider's real native/probe close. No next
        // socket, endpoint or worker is prepared until it confirms this slot.
        let closed = state
            .permit
            .as_ref()
            .unwrap()
            .close(state.receipt.as_ref(), &self.tasks);
        drop(state);
        entry.receive_native_failure().await?;
        closed?;
        let mut state = entry.state.lock().await;
        let failure = state
            .segment
            .as_ref()
            .and_then(|segment| segment.task_exit())
            .filter(|exit| exit.joined.result().is_err())
            .map(|exit| exit.joined.clone());
        drop(state.segment.take());
        drop(state.permit.take());
        state.retiring_registrations = state.receipt.take();
        if let Some(failure) = failure {
            state.retired_application.push(failure.clone());
            // A failed original worker is a real task fault. Retain its actual
            // group for the same host's final report instead of adding an
            // unbounded stream of failures to one still-live logical entry.
            return Err(error(Stage::Close, Reason::Io)
                .with_source(OriginalChannel(failure.result().unwrap_err())));
        }
        // The current hop Aux is independent of this old physical segment.
        // NativeClosed precedes waiting its original packet ACK; the persistent
        // endpoint ledger remains live in the exact returned unique queues.
        drop(state);
        entry.wait_endpoint_ack(end).await?;
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?;
        if entry.cancel.is_cancelled() || state.closed {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let permit = self.metadata[profile]
            .grants()
            .reserve(state.caller.as_ref().unwrap())?;
        state.native_sources = permit.origin().original_sources();
        state.origin = Some(permit.origin());
        state.permit = Some(permit);
        let (peer, grant) = state
            .candidates
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
            .open_metadata(
                state
                    .selected_candidate
                    .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?,
                Some(hop.next_port()),
                state.permit.as_ref().unwrap(),
                state.caller.as_ref().unwrap(),
                end,
            )?;
        state.unbound = Some(grant);
        let queues = state
            .queues
            .take()
            .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
        let segment = match PacketSegment::prepare(
            state.unbound.take().unwrap(),
            queues,
            self.tasks.clone(),
            peer,
            state.logical.unwrap(),
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
        if let Some(auth) = &state.auth {
            // New donor loans use this acquired original. Previously issued
            // pool roots retain their own immutable binding/endpoint lineage.
            *auth.native_origin.lock().unwrap() = state.origin.clone();
        }
        state.next_hop = Some(Instant::now() + hop.next_interval());
        Ok(true)
    }
}

impl EntryState {
    pub(super) fn idle(&self) -> bool {
        self.auth
            .as_ref()
            .is_some_and(|auth| Arc::strong_count(auth) == 1)
    }
}
