use super::*;
use zero_platform_tokio::contract::lifecycle::{CloseReport, Unfinished};
impl HostResources {
    pub(in crate::component::instance::resources::native) fn close_root(
        &self,
        usage: RootDatagramUse,
    ) -> Result<RootDatagramClosed, ChannelError> {
        let context = self.roots.context().ok_or_else(|| self.denied())?;
        let entry = self
            .roots
            .entries
            .get(&usage.request().ordinal())
            .ok_or_else(|| self.denied())?;
        if !context.authority.matches_request(usage.request()) || !usage.view_returned() {
            return Err(self.denied());
        }
        let control = entry
            .controls()
            .into_iter()
            .find_map(|(control, actual)| {
                actual
                    .filter(|actual| actual.same_use(&usage))
                    .map(|_| control)
            })
            .ok_or_else(|| self.denied())?;
        // Revocation, runtime deregistration and external waker Drop all run
        // without any host/resource/native-owner mutex. Same original only.
        let closed = control
            .try_close()
            .ok_or_else(|| {
                error(
                    self.component,
                    Reason::Capacity,
                    "original root UDP native gate is still busy",
                )
            })?
            .map_err(|source| {
                entry
                    .ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .first_close_source
                    .get_or_insert(source.clone());
                let failure = error(self.component, Reason::Io, "root UDP original close failed")
                    .with_source(source);
                let source = Arc::new(failure);
                entry
                    .ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .first_fault
                    .get_or_insert(source.clone());
                crate::component::instance::retained(source)
            })?;
        let receipt = context
            .authority
            .confirm_closed(&usage, &control, &closed)
            .ok_or_else(|| self.denied())?;
        entry.persist_report(self.component);
        Ok(receipt)
    }
}
impl RootDeclarations {
    pub(in crate::component::instance::resources::native) fn revoke_and_close(
        &self,
        component: ComponentId,
    ) {
        self.closed.store(true, Ordering::Release);
        for entry in self.entries.values() {
            let controls = entry.controls();
            for (control, _) in &controls {
                control.revoke();
            }
            for (control, usage) in controls {
                if usage.is_some_and(|usage| usage.view_returned()) {
                    if let Some(Err(source)) = control.try_close() {
                        entry
                            .ledger
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .first_close_source
                            .get_or_insert(source.clone());
                        Self::fault(
                            entry,
                            error(
                                component,
                                Reason::Io,
                                "retired root UDP original close failed",
                            )
                            .with_source(source),
                        );
                    }
                }
            }
            entry.persist_report(component);
            entry.reap_queries(component);
        }
    }
    pub(in crate::component::instance::resources::native) fn append_receipts(
        &self,
        component: ComponentId,
        report: &mut CloseReport,
    ) {
        for (ordinal, entry) in &self.entries {
            let count = std::mem::take(
                &mut entry
                    .ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pending_closed,
            );
            if count != 0 {
                report.closed_io.push(format!(
                    "component {} root UDP declaration {}: {} original closes",
                    component.0, ordinal, count
                ));
            }
        }
        report.unfinished.extend(
            self.evidence(component)
                .into_iter()
                .filter(|entry| !entry.physical_confirmed())
                .map(|entry| {
                    Unfinished::Resource(format!(
                        "component {} root UDP declaration {} native={} views={} query_joins={}",
                        component.0,
                        entry.ordinal,
                        entry.native_confirmed,
                        entry.users_returned,
                        entry.queries_joined
                    ))
                }),
        );
    }
}
impl Entry {
    pub(super) fn reap_queries(&self, component: ComponentId) {
        let snapshot = self.queries();
        let mut completed = Vec::new();
        for query in &snapshot {
            let Some(joined) = query.witness.try_joined() else {
                continue;
            };
            if let Err(source) = joined.result() {
                self.ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .first_query_failure
                    .get_or_insert(joined.clone());
                self.ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .first_fault
                    .get_or_insert(source);
            }
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
            let closed = control.is_some_and(|control| control.observe().native_closed)
                && usage.is_some_and(|usage| usage.view_returned());
            if !opening && (settled || closed) {
                completed.push(query.clone());
            }
        }
        self.persist_report(component);
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut dropped = Vec::new();
        let mut index = 0;
        while index < records.len() {
            if completed
                .iter()
                .any(|old| Arc::ptr_eq(old, &records[index]))
            {
                dropped.push(records.remove(index));
            } else {
                index += 1;
            }
        }
        drop(records);
        // Last controls/witnesses may synchronously drop external callbacks.
        drop(dropped);
    }
}
