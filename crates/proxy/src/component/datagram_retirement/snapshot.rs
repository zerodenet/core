use super::*;
impl RetiredDatagrams {
    pub(super) fn snapshot_all(&self, take: bool) -> CloseReport {
        self.snapshot(take, false)
    }
    pub(super) fn snapshot_retired(&self, take: bool) -> CloseReport {
        self.snapshot(take, true)
    }
    fn snapshot(&self, take: bool, retired_only: bool) -> CloseReport {
        let mut report = CloseReport::default();
        let mut completed = Vec::new();
        for (id, entry) in self.entries() {
            let mut state = entry.lock().expect("Datagram entry poisoned");
            if retired_only && (state.reserved || state.active) {
                continue;
            }
            report.append(state.snapshot(id, take));
            if take && state.closed {
                completed.push((id, entry.clone()))
            }
        }
        self.remove_closed(&completed);
        report
    }
    pub(super) fn remove_closed(&self, completed: &[(u64, Arc<Mutex<Entry>>)]) {
        let mut removed = Vec::new();
        {
            let mut state = self.state.lock().expect("Datagram owner poisoned");
            for (id, entry) in completed {
                if state
                    .entries
                    .get(id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, entry))
                {
                    removed.push(state.entries.remove(id).expect("same closed entry"));
                }
            }
        }
        // Protocol controls, their error sources and registered native wakers
        // can run arbitrary last-reference destructors. All parents are unlocked.
        drop(removed);
    }
}
