use super::*;
#[derive(Clone, Debug)]
pub(in crate::component) struct RootDatagramEvidence {
    pub ordinal: usize,
    pub native_confirmed: bool,
    pub known_no_acquisition: bool,
    pub users_returned: bool,
    pub queries_joined: bool,
    pub admission_closed: bool,
    pub first_fault: Option<Arc<ChannelError>>,
    pub first_query_failure: Option<zero_platform_tokio::contract::scope::TaskJoined>,
    pub first_close_source: Option<Arc<std::io::Error>>,
    pub native_unknown: Option<Arc<std::io::Error>>,
}
impl RootDatagramEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        self.admission_closed
            && (self.native_confirmed || self.known_no_acquisition)
            && self.users_returned
            && self.queries_joined
    }
}
impl RootDeclarations {
    pub(in crate::component::instance::resources::native) fn evidence(
        &self,
        component: ComponentId,
    ) -> Vec<RootDatagramEvidence> {
        self.entries
            .iter()
            .map(|(ordinal, entry)| {
                entry.reap_queries(component);
                let records = entry.queries();
                let mut queries_joined = true;
                let mut users_returned = true;
                let mut native_confirmed = true;
                for query in &records {
                    queries_joined &= query.witness.is_joined();
                    let (opening, settled, control, usage) = {
                        let state = query
                            .attempt
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        (
                            state.opening,
                            state.settled_without_view,
                            state.control.clone(),
                            state.usage.clone(),
                        )
                    };
                    users_returned &=
                        settled || usage.as_ref().is_some_and(RootDatagramUse::view_returned);
                    native_confirmed &= !opening
                        && (settled
                            || control.is_some_and(|control| control.observe().native_closed));
                }
                let native = match entry.native.try_lock() {
                    Ok(mut owner) => Some(owner.take_report()),
                    Err(std::sync::TryLockError::Poisoned(error)) => {
                        Some(error.into_inner().take_report())
                    }
                    Err(std::sync::TryLockError::WouldBlock) => None,
                };
                let mut native_unknown = None;
                let mut ledger = entry
                    .ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(report) = native {
                    native_confirmed &= report.is_complete();
                    ledger.native_unconfirmed = !report.unconfirmed.is_empty();
                    native_unknown = report
                        .unconfirmed
                        .first()
                        .map(|unknown| unknown.source.clone());
                    ledger.pending_closed += report.confirmed_closed;
                    ledger.total_closed += report.confirmed_closed;
                    ledger.known_no_native += report.known_no_native;
                } else {
                    native_confirmed = false;
                }
                // Explicit original-owner acquisition state, not empty query
                // records. NeverAttempted and actual KnownNoNative differ from
                // positive OS close receipts and remain separately published.
                let known_no_acquisition = native_confirmed
                    && ledger.total_closed == 0
                    && (!entry.attempted.load(Ordering::Acquire) || ledger.known_no_native != 0);
                RootDatagramEvidence {
                    ordinal: *ordinal,
                    native_confirmed: native_confirmed && ledger.total_closed != 0,
                    known_no_acquisition,
                    users_returned,
                    queries_joined,
                    admission_closed: self.closed.load(Ordering::Acquire),
                    first_fault: ledger.first_fault.clone(),
                    first_query_failure: ledger.first_query_failure.clone(),
                    first_close_source: ledger.first_close_source.clone(),
                    native_unknown,
                }
            })
            .collect()
    }
}
