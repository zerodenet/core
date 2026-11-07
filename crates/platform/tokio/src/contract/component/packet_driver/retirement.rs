use super::*;
use std::sync::atomic::Ordering;
impl Record {
    pub(super) fn released(&self) -> bool {
        if self.failures.failed() && !self.delivery.acknowledged() {
            return false;
        }
        if let Some(installed) = self.installed.get() {
            if let Ok(owner) = installed.control.try_lock() {
                if owner.closed().is_some_and(|same| same.matches(&owner)) {
                    installed.complete.store(true, Ordering::Release);
                }
            }
        }
        if !self.call.cancel.is_cancelled()
            || self
                .installed
                .get()
                .is_some_and(|owner| !owner.complete.load(Ordering::Acquire))
        {
            return false;
        }
        // Cancellation precedes the table snapshots. Admission rechecks it
        // under the same table mutex, so an earlier admitted grant is visible
        // here and an uninstalled original remains incomplete.
        let stream_count = self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        for index in 0..stream_count {
            let Some(stream) = self.stream_at(index) else {
                return false;
            };
            if !stream.complete() {
                return false;
            }
        }
        let pending_count = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        for index in 0..pending_count {
            let Some(original) = self.pending_at(index) else {
                return false;
            };
            if !original.released() {
                return false;
            }
        }
        let complete = self
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            == pending_count
            && self
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                == stream_count;
        if complete {
            self.settled.store(true, Ordering::Release);
        }
        complete
    }
    pub(super) async fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.request_retirement();
        let mut report = CloseReport::default();
        let mut failure = None;
        if let Some(installed) = self.installed.get() {
            match tokio::time::timeout_at(deadline, installed.control.lock()).await {
                Ok(mut control) => {
                    if control.closed().is_some_and(|same| same.matches(&control)) {
                        installed.complete.store(true, Ordering::Release);
                    }
                    let result = match self.failed_phase(&self.failures.final_control) {
                        Some(source) if !control.logical_cleanup_continuation() => Err(source),
                        _ if installed.complete.load(Ordering::Acquire) => {
                            Ok(CloseReport::default())
                        }
                        _ => self.receive_phase(
                            &self.failures.final_control,
                            control.close(deadline).await,
                        ),
                    };
                    if result.as_ref().is_ok_and(CloseReport::is_complete) {
                        installed.complete.store(true, Ordering::Release);
                    }
                    match result {
                        Ok(closed) => report.append(closed),
                        Err(source) => {
                            failure.get_or_insert(source);
                        }
                    }
                }
                Err(_) => report.unfinished.push(Unfinished::Resource(
                    "same Packet driver logical control retained".into(),
                )),
            }
        }
        // The installed same logical control has been driven after its unique
        // views returned. Continue every actual pending native/auxiliary phase
        // even on a control fault; no parent-wire ACK blocks this progress.
        for index in 0..self.max_pending {
            let Some(adoption) = self.pending_at(index) else {
                break;
            };
            match adoption.close_native(self, deadline).await {
                Ok(true) => {}
                Ok(false) => report.unfinished.push(Unfinished::Resource(
                    "same Packet caller native original remains unconfirmed".into(),
                )),
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        // Physical child controls/IO are driven first: those exact owners may
        // still hold a prefix Stream's unique IO. Waiting on prefix return
        // before driving them would create a real child-parent cleanup cycle.
        for index in 0..self.packet_originals.len() {
            let Some(stream) = self.stream_at(index) else {
                break;
            };
            match stream.close(deadline).await {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        // This attempt owns every original through its sealed concrete Record.
        // Each callee's own view/native/Aux/driver/ACK must settle. The long
        // Source actor remains charged by its independent actual owner.
        for index in 0..self.max_pending {
            let Some(adoption) = self.pending_at(index) else {
                break;
            };
            match adoption.close_logical(self, deadline).await {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    failure.get_or_insert(source);
                }
            }
        }
        if !self.released() {
            report.unfinished.push(Unfinished::Resource(
                "same Packet attempt originals retain cleanup or fault delivery debt".into(),
            ));
        }
        match failure {
            Some(source) => Err(source),
            None => Ok(report),
        }
    }
}
impl PacketDriverCalls {
    /// Positive loans/children/reconnect registration evidence only. The
    /// Source's original main/wire/native/whole Scope remain independent.
    pub fn cleanup_settled(&self) -> bool {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
            && self.current_delivery().acknowledged_or_empty()
            && self.source_failures.accepted_or_empty()
            && self
                .reconnects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .all(|actor| actor.registration_released() && actor.fault_received())
    }
    pub fn request_retirement(&self) {
        self.retiring.store(true, Ordering::Release);
        for index in 0..self.max_calls.get() {
            let Some(record) = self.call_at(index) else {
                break;
            };
            record.request_retirement();
        }
    }
    pub async fn shutdown(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.request_retirement();
        let mut report = CloseReport::default();
        let mut first = None;
        for index in 0..self.max_calls.get() {
            let Some(record) = self.call_at(index) else {
                break;
            };
            match record.close(deadline).await {
                Ok(closed) => report.append(closed),
                Err(source) => {
                    first.get_or_insert(source);
                }
            }
        }
        self.remove_released();
        if !self
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
        {
            report.unfinished.push(Unfinished::Resource(
                "same Packet driver originals retain cleanup or fault delivery debt".into(),
            ));
        }
        let actor_failure = self.receive_actor_failures();
        if first.is_none() {
            first = actor_failure
                .or_else(|| self.source_failures.report())
                .or_else(|| self.current_delivery().report());
        }
        match first {
            Some(_) if self.cleanup_settled() && report.is_complete() => Ok(report),
            Some(source) => Err(source),
            None => Ok(report),
        }
    }
}
