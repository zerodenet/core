use super::*;
impl ControlledIcmpOwner {
    pub(super) fn reserve(&mut self, ipv6: bool) -> Result<Arc<Entry>, IcmpAcquisitionError> {
        self.reserve_for(ipv6, None)
    }
    pub(super) fn reserve_for(
        &mut self,
        ipv6: bool,
        caller: Option<crate::contract::scope::TaskJoinWitness>,
    ) -> Result<Arc<Entry>, IcmpAcquisitionError> {
        self.reserve_observed(ipv6, caller, None)
    }
    pub(super) fn reserve_observed(
        &mut self,
        ipv6: bool,
        caller: Option<crate::contract::scope::TaskJoinWitness>,
        receiver: Option<
            crate::controlled_datagram::metadata_pool::MetadataPoolRegistrationObservation,
        >,
    ) -> Result<Arc<Entry>, IcmpAcquisitionError> {
        self.reap();
        let denied = |message| IcmpAcquisitionError {
            raw_permission_source: None,
            reservation: None,
            outcome: IcmpAcquisitionOutcome::KnownNoNative,
            source: Arc::new(std_io::Error::other(message)),
        };
        if self.cancel.is_cancelled() {
            return Err(denied("ICMP owner is revoked"));
        }
        if let Some(entry) = self
            .entries
            .values()
            .find(|entry| matches!(&*entry.native(), Native::Unknown(_)))
        {
            let native = entry.native();
            let Native::Unknown(source) = &*native else {
                unreachable!()
            };
            return Err(IcmpAcquisitionError {
                raw_permission_source: entry.metadata().raw_permission_source.clone(),
                reservation: Some(entry.id),
                outcome: IcmpAcquisitionOutcome::Unknown(entry.id),
                source: source.clone(),
            });
        }
        if self.entries.len() >= self.limit.get() {
            return Err(denied("ICMP original capacity exhausted"));
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or_else(|| denied("ICMP identity exhausted"))?;
        let entry = Arc::new(Entry {
            wakes: relay::IoRelays::new(),
            owner: self.identity.clone(),
            id: IcmpReservationId(self.next),
            // Host cancellation still rejects new reserve. Only this observed
            // already-issued unique view survives until its original receiver
            // returns it after the final mask/endpoint drain.
            cancel: if receiver.is_some() {
                CancellationToken::new()
            } else {
                self.cancel.child_token()
            },
            metadata_receiver: receiver,
            native: Mutex::new(Native::Reserved),
            uncertain: Arc::new(std_io::Error::other(
                "ICMP original native outcome is unconfirmed",
            )),
            creation_running: std::sync::atomic::AtomicBool::new(true),
            view_live: std::sync::atomic::AtomicBool::new(false),
            changed: self.identity.clone(),
            ipv6,
            metadata: Mutex::new(Metadata::default()),
        });
        if let Some(binding) = &self.totals {
            binding
                .reserve(&entry, caller)
                .map_err(|source| IcmpAcquisitionError {
                    raw_permission_source: None,
                    reservation: None,
                    outcome: IcmpAcquisitionOutcome::KnownNoNative,
                    source: Arc::new(std_io::Error::new(std_io::ErrorKind::WouldBlock, source)),
                })?;
        }
        self.entries.insert(self.next, entry.clone());
        self.next = next;
        Ok(entry)
    }
}
