//! Capture pure actual foreign declaration data before any native acquisition.
use super::*;
impl Root {
    pub(super) fn retain_original_pending(
        &self,
        binding: &UpstreamBinding,
        adoption: zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_pending(binding, &adoption)?;
        let ceiling = self.pending_carrier_capacity(binding).ok_or_else(|| {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "DNS pending graph capacity is no longer available",
            )
        })?;
        let source = self.current.upgrade().ok_or_else(|| {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::Revoked,
                "actual retained DNS source was withdrawn",
            )
        })?;
        let mut failure = None;
        let mut actual_sources = None;
        for role in &self.roles {
            match source.owned_carrier_declared_sources(*role, &adoption) {
                Ok(sources) => {
                    actual_sources = Some(sources);
                    break;
                }
                Err(source) => {
                    failure = Some(source);
                }
            }
        }
        let sources = actual_sources.ok_or_else(|| {
            failure.map(error).unwrap_or_else(|| {
                crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "actual pending original has no retained role/source receipt",
                )
            })
        })?;
        if sources.instance() != adoption.instance() {
            return Err(crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "actual pending original declaration sources differ",
            ));
        }
        let receiver = self.receiver.clone().with_declared_sources(sources);
        let candidates = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .filter(|entry| entry.task.same_registration(&self.caller))
            .cloned()
            .collect::<Vec<_>>();
        let closed = candidates
            .into_iter()
            .filter(|entry| {
                entry
                    .original
                    .closed()
                    .is_some_and(|closed| closed.matches(&entry.original))
                    && entry
                        .original
                        .users_closed()
                        .is_some_and(|closed| closed.matches(&entry.original))
            })
            .collect::<Vec<_>>();
        let (removed, outcome) = {
            let mut pending = self.pending.lock().expect("DNS pending originals poisoned");
            let mut removed = Vec::new();
            let mut index = 0;
            while index < pending.len() {
                if closed
                    .iter()
                    .any(|entry| Arc::ptr_eq(entry, &pending[index]))
                {
                    removed.push(pending.remove(index));
                } else {
                    index += 1;
                }
            }
            let same = pending.iter().any(|entry| {
                entry.task.same_registration(&self.caller) && entry.original.same_slot(&adoption)
            });
            let outcome = if same {
                Ok(())
            } else if pending
                .iter()
                .filter(|entry| entry.task.same_registration(&self.caller))
                .count()
                >= ceiling
            {
                Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::Capacity,
                    "DNS actual pending carrier path is full",
                ))
            } else {
                pending.push(Arc::new(custody::NativeUse {
                    receiver,
                    pending_report: Mutex::default(),
                    protocol_originals: Mutex::default(),
                    task: self.caller.clone(),
                    cancel: self.cancel.clone(),
                    original: adoption,
                    capacity: Mutex::new(None),
                    pending_roles: self.roles.clone().into(),
                    logical: Default::default(),
                }));
                Ok(())
            };
            (removed, outcome)
        };
        // Original control/payload Drop can wake foreign code. Never do that
        // while the caller's actual pending ledger is locked.
        drop(removed);
        outcome
    }
}
