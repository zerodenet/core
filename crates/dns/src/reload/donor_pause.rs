//! The existing actual reload token holds its query pause through commit/discard.
use super::*;
impl DnsSystem {
    /// Attach only the concrete target journal envelope to this same token.
    /// Its actual allocator pause and compaction are driven before StopOld.
    pub fn prepare_fake_ip_journal_limit(
        &self,
        resources: &DnsReloadResources,
        limit: usize,
    ) -> io::Result<()> {
        if limit == 0 || limit.checked_add(1).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid journal recovery envelope",
            ));
        }
        let prepared = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload poisoned");
        if !prepared
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, &resources.0))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal envelope requires the exact DNS candidate",
            ));
        }
        let mut target = resources
            .0
            .fake_ip_journal_limit
            .lock()
            .expect("journal limit poisoned");
        if target.is_some_and(|old| old != limit) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "DNS token already holds another journal envelope",
            ));
        }
        *target = Some(limit);
        Ok(())
    }
    pub fn prepare_donor_pause(
        &self,
        resources: &DnsReloadResources,
        replaced: &[zero_core::component::ComponentInstanceId],
    ) -> io::Result<()> {
        {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other(
                    "DNS donor pause requires its actual current reload token",
                ));
            }
            let pause = resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned");
            if let Some(pause) = pause.as_ref() {
                return if pause.matches_replacement(replaced) {
                    Ok(())
                } else {
                    Err(io::Error::other(
                        "actual DNS token already names different replaced originals",
                    ))
                };
            }
        }
        // Preparing may inspect/join-observe an original query and release
        // actual witnesses. No token/cache/state lock spans these callbacks.
        let pause =
            Arc::new(self.pause_donor_replacement(replaced, resources.0.native_profile_changed)?);
        {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other("DNS donor pause token was discarded"));
            }
            let mut installed = resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned");
            if let Some(current) = installed.as_ref() {
                if !current.matches_replacement(replaced) {
                    return Err(io::Error::other(
                        "DNS donor pause token changed its actual replacement set",
                    ));
                }
            } else {
                *installed = Some(pause.clone());
            }
        }
        // A concurrent duplicate preparation restores only its own pause count.
        drop(pause);
        Ok(())
    }
    pub async fn retire_paused_donors(
        &self,
        resources: &DnsReloadResources,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        let pause = {
            let prepared = self
                .prepared_reload
                .lock()
                .expect("DNS prepared reload lock poisoned");
            if !prepared
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &resources.0))
            {
                return Err(io::Error::other(
                    "DNS donor retirement requires its actual current reload token",
                ));
            }
            resources
                .0
                .donor_pause
                .lock()
                .expect("DNS donor pause poisoned")
                .clone()
                .ok_or_else(|| io::Error::other("actual DNS donor pause was not installed"))?
        };
        // Cancellation retains this same guard in prepared_reload. No mutex,
        // Scope join-consumer, or fresh resource owner crosses this await.
        // Normal DNS profile changes cancel the actual old flights only after
        // their query registration fences are retained in this same token.
        pause.cancel_queries(self)?;
        pause.retire(end).await?;
        let limit = *resources
            .0
            .fake_ip_journal_limit
            .lock()
            .expect("journal limit poisoned");
        if pause.has_fake_ip_journal() {
            if let Some(limit) = limit {
                if let Some(allocator) = self.snapshot_fake_ip() {
                    allocator.compact_for_limit(limit)?;
                }
            }
        }
        Ok(())
    }
}
