use super::*;
use std::sync::atomic::Ordering;
impl PacketDriverCalls {
    /// Inline advancement of existing originals. No table snapshot, new worker
    /// or inferred completion from a disappearing caller is involved.
    pub fn collect_retired(&self) {
        for index in 0..self.max_calls.get() {
            let Some(record) = self.call_at(index) else {
                break;
            };
            if !record.call.cancel.is_cancelled() {
                continue;
            }
            if let Some(installed) = record.installed.get() {
                record.release_host_retained_pending(installed);
            }
            for index in 0..record.max_pending {
                let Some(adoption) = record.pending_at(index) else {
                    break;
                };
                let _ = adoption.try_native(&record);
            }
            if let Some(installed) = record.installed.get() {
                if let Ok(mut control) = installed.control.try_lock() {
                    if control.closed().is_some_and(|same| same.matches(&control)) {
                        installed.complete.store(true, Ordering::Release);
                    }
                    let result = match record.failed_phase(&record.failures.final_control) {
                        Some(_) if !control.logical_cleanup_continuation() => None,
                        _ if installed.complete.load(Ordering::Acquire) => None,
                        _ => Some(control.try_close()),
                    };
                    if let Some(result) = result {
                        if result.as_ref().is_ok_and(CloseReport::is_complete) {
                            installed.complete.store(true, Ordering::Release);
                        }
                        let _ = record.receive_phase(&record.failures.final_control, result);
                    }
                }
            }
            for index in 0..record.packet_originals.len() {
                let Some(stream) = record.stream_at(index) else {
                    break;
                };
                let _ = stream.try_close();
            }
            for index in 0..record.max_pending {
                let Some(adoption) = record.pending_at(index) else {
                    break;
                };
                let _ = adoption.try_logical(&record);
            }
        }
        // Concurrent handoff/removal can defer observation, never drop debt.
        // Only fresh evidence of the entire current row authorizes removal.
        self.remove_released();
    }
}
