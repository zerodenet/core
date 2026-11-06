//! Snapshot exact original controls before crossing native/waker callbacks.
use super::*;
/// Same MetadataOwner originals, not another owner or a closed receipt. Keep
/// the snapshot through report harvest, then drop it outside bookkeeping locks.
pub struct MetadataNativeClose {
    entries: Vec<Arc<Entry>>,
    pollers: Vec<Arc<poller::Broker>>,
}
impl MetadataOwner {
    /// Pure original-Arc snapshots only; no cancellation, IO, close or waker.
    pub fn prepare_native_close(&self) -> MetadataNativeClose {
        MetadataNativeClose {
            entries: self
                .main
                .entries
                .values()
                .chain(self.probes.owner.entries.values())
                .cloned()
                .collect(),
            pollers: self
                .views
                .values()
                .map(|shared| shared.pollers.clone())
                .collect(),
        }
    }
}
impl MetadataNativeClose {
    /// Must run outside enclosing owner/resource locks. Each original performs
    /// its existing guarded close, retaining Unknown and exact native sources.
    /// All native reports remain in their original MetadataOwner for harvest.
    pub fn try_close(&self) -> Option<Arc<std_io::Error>> {
        for entry in &self.entries {
            entry.retire_io();
        }
        for pollers in &self.pollers {
            pollers.signal();
        }
        let mut first = None;
        for entry in &self.entries {
            if let Some(Err(source)) = entry.try_close() {
                first.get_or_insert(source);
            }
        }
        first
    }
}
