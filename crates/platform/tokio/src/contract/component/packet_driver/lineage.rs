//! A real carrier invocation keeps its identity across finite reconnect tasks.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct Lineage {
    pub(super) wire: TaskJoinWitness,
    pub(super) retiring: AtomicBool,
}

#[cfg(test)]
#[path = "tests/lineage.rs"]
mod tests;
impl PacketDriverCalls {
    pub(super) fn wire_for_actor(
        &self,
        actor: &TaskJoinWitness,
    ) -> Result<TaskJoinWitness, ChannelError> {
        if self.original_actor(actor) {
            return Ok(actor.clone());
        }
        self.reconnects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|registered| {
                registered
                    .witness
                    .get()
                    .is_some_and(|same| same.same_registration(actor))
            })
            .map(|registered| registered.wire.clone())
            .ok_or_else(|| failure(Reason::StalePlan))
    }

    pub(super) fn new_lineage(
        &self,
        actor: &TaskJoinWitness,
    ) -> Result<MetadataRef<Lineage>, ChannelError> {
        let value = Lineage {
            wire: self.wire_for_actor(actor)?,
            retiring: false.into(),
        };
        #[cfg(feature = "native-packet-origin")]
        {
            let credit = self
                .metadata_memory()?
                .reserve_driver_metadata(
                    MetadataRef::<Lineage>::layout().map_err(memory::memory_error)?,
                )
                .map_err(memory::memory_error)?;
            MetadataRef::from_prepaid(value, credit)
                .map_err(|(source, _, _)| memory::memory_error(source))
        }
        #[cfg(not(feature = "native-packet-origin"))]
        {
            Ok(Arc::new(value))
        }
    }

    pub(super) fn request_lineage_retirement(&self, lineage: &MetadataRef<Lineage>) {
        // Admission rechecks this same flag under the calls table lock. Every
        // operation admitted before the fence is retained in that same table.
        lineage.retiring.store(true, Ordering::Release);
        for _ in 0..self.max_calls.get() {
            let record = self
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .find(|record| {
                    MetadataRef::ptr_eq(&record.lineage, lineage)
                        && !record.call.cancel.is_cancelled()
                })
                .cloned();
            let Some(record) = record else { break };
            // Cancellation callbacks and provider code never run under the
            // calls table mutex. Selecting the next uncancelled original also
            // remains correct when a concurrent collector compacts the table.
            record.request_retirement();
        }
    }

    /// Exact call history, including partially built or cancelled attempts.
    /// Source task joins and shared ancestors' native receipts are separate.
    pub(super) async fn retire_lineage_attempts(
        &self,
        lineage: &MetadataRef<Lineage>,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        let mut report = CloseReport::default();
        let mut first = None;
        for index in 0..self.max_calls.get() {
            let Some(record) = self.call_at(index) else {
                break;
            };
            if !MetadataRef::ptr_eq(&record.lineage, lineage) {
                continue;
            }
            match record.close(deadline).await {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    first.get_or_insert(source);
                }
            }
        }
        self.remove_released();
        // Concurrent collection can compact the table during the scan. Such
        // a race may postpone closure; it must never produce a false success.
        let retained = self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|record| MetadataRef::ptr_eq(&record.lineage, lineage));
        if retained {
            report.unfinished.push(Unfinished::Resource(
                "same carrier call retains original attempt cleanup or fault-delivery debt".into(),
            ));
        } else if self.source_failures.accepted_or_empty() {
            // No row disappears without its own complete actual receipts.
            // This fresh census supersedes stale per-row unfinished snapshots;
            // faults remain canonical ResourceFailures in the same Source.
            report.unfinished.clear();
            if first
                .as_ref()
                .is_none_or(|source| self.source_failures.owns(source))
            {
                if let Some(failure) = self
                    .source_failures
                    .closed_failure(self.original.instance())
                {
                    if !report
                        .resource_failures
                        .iter()
                        .any(|same| Arc::ptr_eq(same, &failure))
                    {
                        report.resource_failures.push(failure);
                    }
                    return Ok(report);
                }
            }
        } else {
            report.unfinished.push(Unfinished::Resource(
                "same Source cleanup faults await actual receiver custody".into(),
            ));
        }
        match first {
            Some(source) => Err(source),
            None => Ok(report),
        }
    }
}
