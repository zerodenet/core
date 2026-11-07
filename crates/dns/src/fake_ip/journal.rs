//! Activate only the exact Default owner's journal. A retained allocator keeps
//! its valid history when that concrete provider/Scope is replaced.
use super::*;
impl FakeIpAllocator {
    pub(crate) fn pause_journal(&self) -> io::Result<Option<persistence::JournalPause>> {
        let state = self.inner.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "an actual Fake-IP operation remains live",
            )
        })?;
        if let Some(persistence) = &state.persistence {
            persistence.ensure_next_generation()?;
            Ok(Some(persistence.pause()?))
        } else {
            Ok(None)
        }
    }
    pub(crate) fn compact_for_limit(&self, limit: usize) -> io::Result<()> {
        let state = self.inner.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "an actual Fake-IP operation remains live",
            )
        })?;
        let Some(persistence) = &state.persistence else {
            return Ok(());
        };
        if !persistence.paused() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "journal compaction requires its actual query pause",
            ));
        }
        let checkpoint = checkpoint::Checkpoint::new(&state, None, None)?;
        if persistence.checkpoint_bytes(&checkpoint)? > limit {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "valid Fake-IP history exceeds the candidate recovery envelope",
            ));
        }
        if persistence.bytes() > limit {
            persistence.compact_checkpoint(&checkpoint)?;
        }
        Ok(())
    }
    pub(crate) fn activate_persistence(&self) -> io::Result<()> {
        let mut state = self.inner.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "Fake-IP allocator still has an actual operation",
            )
        })?;
        let Some(persistence) = state.persistence.as_ref() else {
            return Ok(());
        };
        if !persistence.activation_needed() {
            return Ok(());
        }
        if !persistence.initialized() {
            if !state.forward.is_empty() || !state.retired.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unstarted Fake-IP journal unexpectedly has live state",
                ));
            }
            let (persistence, recovered) = persistence.reopen()?;
            let mut restored = AllocatorState {
                next_ipv4: u32::from(self.ipv4_network.network()) + 1,
                next_ipv6: self
                    .ipv6_network
                    .map(|network| u128::from(network.network())),
                clock: 0,
                forward: HashMap::new(),
                reverse: HashMap::new(),
                retired: HashMap::new(),
                persistence: Some(persistence),
            };
            let valid = restore_mappings(
                &mut restored,
                &self.ipv4_network,
                self.ipv6_network.as_ref(),
                self.ttl,
                self.max_entries,
                recovered.mappings,
            )
            .and_then(|()| {
                restore_retired_addresses(
                    &mut restored,
                    &self.ipv4_network,
                    self.ipv6_network.as_ref(),
                    self.ttl,
                    recovered.retired,
                )
            });
            if let Err(error) = valid {
                tracing::warn!(%error, "discarding invalid Fake-IP persistence state");
                restored.forward.clear();
                restored.reverse.clear();
                restored.retired.clear();
                restored.persistence.as_mut().unwrap().compact(&[], &[])?;
            }
            *state = restored;
        } else {
            // Rollback/provider refresh never reloads or clears valid history.
            // Rebind rewrites this same allocator only after old native close.
            let AllocatorState {
                forward,
                retired,
                persistence,
                ..
            } = &mut *state;
            let checkpoint = checkpoint::Checkpoint::from_maps(forward, retired, None, None)?;
            persistence
                .as_mut()
                .unwrap()
                .rebind_checkpoint(&checkpoint)?;
        }
        Ok(())
    }
}
