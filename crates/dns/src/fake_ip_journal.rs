//! Concrete hosted journal activation; no standalone production fallback.
use super::*;
use zero_platform_tokio::{contract::scope::TaskRegistrar, journal::JournalGrant};
impl DnsSystem {
    /// The existing kernel's frozen state location; observing it grants no IO.
    /// Configuration-document replacement does not move valid mapping history.
    pub fn fake_ip_journal_path(&self) -> Option<&std::path::Path> {
        self.fake_ip_state_path.as_deref()
    }

    /// Check before opening the replacement journal, preserving the active
    /// original if this kernel cannot issue another persistence generation.
    pub fn ensure_fake_ip_journal_startable(&self) -> io::Result<()> {
        let lease = self
            .fake_ip_state_lease
            .lock()
            .expect("Fake-IP lease poisoned")
            .clone();
        lease.map_or(Ok(()), |lease| lease.ensure_next_generation())
    }
    /// Called by the actual Default query Owner, before it publishes admission.
    /// Node providers never call this or declare another path/lock.
    pub fn install_fake_ip_journal(
        &self,
        grant: JournalGrant,
        tasks: &TaskRegistrar,
    ) -> io::Result<()> {
        if !self.hosted_fake_ip_journal
            || !grant
                .host_scope()
                .is_some_and(|(_, scope)| scope.matches(tasks))
            || self.fake_ip_state_path.as_deref() != Some(grant.path())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Fake-IP journal is not this actual hosted Default resource",
            ));
        }
        let lease = {
            let mut lease = self
                .fake_ip_state_lease
                .lock()
                .expect("Fake-IP lease poisoned");
            lease
                .get_or_insert_with(|| fake_ip::StateLease::hosted(grant.path().to_owned()))
                .clone()
        };
        lease.bind(grant)?;
        // A replacement's prepared DNS config owns its later commit. Opening a
        // candidate must not overwrite a still-committed journal before then.
        let initial = self
            .prepared_reload
            .lock()
            .expect("DNS prepared reload poisoned")
            .is_none();
        if initial {
            if let Some(allocator) = self.snapshot_fake_ip() {
                allocator.activate_persistence()?;
            }
        }
        Ok(())
    }
    pub(crate) fn pause_fake_ip_journal(
        &self,
        replaced: &[zero_core::component::ComponentInstanceId],
    ) -> io::Result<Option<fake_ip::persistence::JournalPause>> {
        let lease = self
            .fake_ip_state_lease
            .lock()
            .expect("Fake-IP lease poisoned")
            .clone();
        let Some(lease) = lease else {
            return Ok(None);
        };
        if !lease.bound() {
            return Ok(None);
        }
        let grant = lease.grant()?;
        if grant
            .host_scope()
            .is_none_or(|(instance, _)| !replaced.contains(&instance))
        {
            return Ok(None);
        }
        self.snapshot_fake_ip()
            .map_or(Ok(None), |allocator| allocator.pause_journal())
    }
    /// Explicit genuine library retirement. Production native/Scope closure is
    /// the concrete journal Host's responsibility and cannot be bypassed here.
    pub fn close_standalone_fake_ip_journal(&self) -> io::Result<bool> {
        let lease = self
            .fake_ip_state_lease
            .lock()
            .expect("Fake-IP lease poisoned")
            .clone();
        lease.map_or(Ok(true), |lease| lease.close_standalone())
    }
}
