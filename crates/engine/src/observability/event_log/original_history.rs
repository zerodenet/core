//! Borrow the actual history without materializing payloads or a selection Vec.
use super::*;
pub struct OriginalEventHistory<'a> {
    history: std::sync::MutexGuard<'a, budget::History>,
    log: &'a EngineEventLog,
}
impl OriginalEventHistory<'_> {
    /// Serialize these same immutable originals while this history guard is
    /// held. The actual HTTP owner measures/prepays its output before encoding;
    /// no selection Vec, event payload or event String is copied here.
    pub fn snapshot_body(&self) -> impl serde::Serialize + '_ {
        SnapshotBody(self)
    }
    pub fn core_instance_id(&self) -> &str {
        &self.log.core_instance_id
    }
    pub fn latest_sequence(&self) -> u64 {
        self.history
            .back()
            .and_then(|event| event.sequence)
            .unwrap_or(0)
    }
    /// The caller must acquire its real output custody before retaining this Arc.
    /// No payload is cloned and no native/task authority is conveyed.
    pub fn next(&self, since: u64) -> (u64, bool, Option<&Arc<RawApiEvent>>) {
        let requested = since.saturating_add(1);
        let first = self
            .history
            .front()
            .and_then(|event| event.sequence)
            .unwrap_or_else(|| {
                self.log
                    .next_sequence
                    .load(Ordering::Relaxed)
                    .max(requested)
            });
        let gap = first > requested;
        let event = self
            .history
            .iter()
            .find(|event| event.sequence.is_some_and(|seq| seq > since));
        let actual =
            event
                .and_then(|event| event.sequence)
                .unwrap_or(if gap { first } else { requested });
        (actual, gap, event)
    }
    pub fn latest_engine_started(&self) -> Option<&Arc<RawApiEvent>> {
        self.history
            .iter()
            .rev()
            .find(|event| event.event_type == event_type::ENGINE_STARTED)
    }
}
struct SnapshotBody<'a, 'history>(&'a OriginalEventHistory<'history>);
impl serde::Serialize for SnapshotBody<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.0.history.len()))?;
        for original in self.0.history.iter() {
            sequence.serialize_element(original.as_ref())?;
        }
        sequence.end()
    }
}
impl EngineEventLog {
    pub(crate) fn original_history(&self) -> OriginalEventHistory<'_> {
        OriginalEventHistory {
            history: self.inner.lock().expect("engine event log lock poisoned"),
            log: self,
        }
    }
}
