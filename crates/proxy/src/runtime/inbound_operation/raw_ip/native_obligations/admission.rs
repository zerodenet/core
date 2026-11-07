//! Exact pre-admission phase; this is never native/Scope closure evidence.
use super::*;
pub(crate) struct SessionAdmission {
    registration: NativeRegistration,
    admitted: bool,
}
impl SessionAdmission {
    pub(crate) fn admitted(mut self) {
        self.admitted = true;
    }
}
impl Drop for SessionAdmission {
    fn drop(&mut self) {
        let Some(owner) = self.registration.owner.upgrade() else {
            return;
        };
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(record) = state.records.get_mut(&self.registration.id) else {
            return;
        };
        record.admission_active -= 1;
        record.business_admitted |= self.admitted;
        record.no_business_admission = record.admission_active == 0 && !record.business_admitted;
    }
}
impl NativeRegistration {
    pub(crate) fn begin_session_admission(&self) -> Result<SessionAdmission, ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if !record.entered || record.retiring || record.kind == ChildKind::Dns {
            return Err(rejected(Reason::InvalidConfig));
        }
        record.admission_active = record
            .admission_active
            .checked_add(1)
            .ok_or_else(|| rejected(Reason::Capacity))?;
        record.no_business_admission = false;
        Ok(SessionAdmission {
            registration: self.clone(),
            admitted: false,
        })
    }
    pub(super) fn settle_pre_admission(&self) -> Result<Option<bool>, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(Some(false));
        };
        let caller = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let Some(record) = state.records.get(&self.id) else {
                return Ok(Some(true));
            };
            if !record.no_business_admission {
                return Ok(None);
            }
            if !record.retiring
                || record.admission_active != 0
                || record.business_admitted
                || record.gateways != 0
                || record.uncovered_gateway
                || record.carrier_conflict
                || record.dns.is_some()
                || record.carrier.is_some()
                || !record.additional_carriers.is_empty()
                || !record.pending_carriers.is_empty()
                || record.datagram.is_some()
            {
                return Ok(None);
            }
            record.caller.clone()
        };
        let Some(caller) = caller else {
            return Ok(Some(false));
        };
        let Some(joined) = caller.observed_join() else {
            return Ok(Some(false));
        };
        let Some(witness) = caller.join_witness() else {
            return Ok(Some(false));
        };
        let actual = witness.try_joined();
        // No admission/gateway was issued by the actual session phase. This
        // does not excuse its OS metadata HANDLE, blocking join or ACK, and
        // never says the shared Source/main/native original has closed.
        Ok(Some(
            actual.is_some_and(|actual| {
                actual.id() == joined.task()
                    && actual.class() == joined.class()
                    && actual.registration_released()
            }) && caller.process_retired(),
        ))
    }
}
