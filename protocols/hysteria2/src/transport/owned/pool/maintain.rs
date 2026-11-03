use super::*;
impl Generation {
    pub(super) async fn maintain(&self, end: Instant) -> Result<(), ChannelError> {
        let entries = self
            .entries
            .lock()
            .unwrap()
            .iter()
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect::<Vec<_>>();
        for (key, entry) in entries {
            let Ok(state) = entry.state.try_lock() else {
                continue;
            };
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
            if failed || (idle && expired) {
                entry.close(end).await?;
                self.remove_entry(&key, &entry);
            } else if hop {
                self.rotate(key.profile, &entry, end).await?;
            }
        }
        Ok(())
    }
    async fn rotate(
        &self,
        profile: usize,
        entry: &Entry,
        end: Instant,
    ) -> Result<(), ChannelError> {
        let options = &self.profiles[profile];
        let Some(hop) = &options.leaf.node.udp_hop else {
            return Ok(());
        };
        let mut state = tokio::time::timeout_at(end, entry.state.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if entry.cancel.is_cancelled() || state.closed {
            return Err(error(Stage::Prepare, Reason::Revoked));
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
        state
            .permit
            .as_ref()
            .unwrap()
            .close(state.receipt.as_ref(), &self.tasks)?;
        drop(state.segment.take());
        drop(state.permit.take());
        state.receipt = None;
        let permit = self.metadata[profile]
            .grants()
            .reserve(state.caller.as_ref().unwrap())?;
        state.origin = Some(permit.origin());
        state.permit = Some(permit);
        let (peer, grant) = state
            .candidates
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
            .open_metadata(
                0,
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
        state.next_hop = Some(Instant::now() + hop.next_interval());
        Ok(())
    }
}

impl EntryState {
    pub(super) fn idle(&self) -> bool {
        self.auth
            .as_ref()
            .is_some_and(|auth| Arc::strong_count(auth) == 1)
            && self.owner.as_ref().is_some_and(|owner| {
                owner.active_users()
                    <= self.baseline
                        + usize::from(
                            self.auth
                                .as_ref()
                                .is_some_and(|auth| auth.udp_witness().is_some()),
                        )
            })
    }
}
