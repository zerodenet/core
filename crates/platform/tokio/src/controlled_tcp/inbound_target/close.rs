//! Native, unique view, actual join, fault reception and HostACK are distinct.
use super::*;
impl InboundTargetNativeOwner {
    pub async fn close_candidate(
        &self,
        index: usize,
        deadline: Instant,
    ) -> Result<bool, ChannelError> {
        let mut state = self.0.state.lock().await;
        let slot = state
            .slots
            .get_mut(&index)
            .ok_or_else(|| failure(Stage::Close, Reason::StalePlan))?;
        let result = slot
            .control
            .as_mut()
            .ok_or_else(|| failure(Stage::Close, Reason::Internal))?
            .wait_closed(deadline)
            .await;
        match result {
            Ok(report) => {
                let complete = report.is_complete();
                let retired = state.collect(index, report);
                drop(state);
                drop(retired);
                if !complete {
                    self.0.stop.cancel();
                }
                Ok(complete)
            }
            Err(source) => {
                let source = state
                    .first_fault
                    .get_or_insert_with(|| Arc::new(source))
                    .clone();
                drop(state);
                self.0.stop.cancel();
                Err(retained(source))
            }
        }
    }
    pub async fn close_cancelled_candidates(&self, deadline: Instant) -> Result<(), ChannelError> {
        let selected = {
            let state = self.0.state.lock().await;
            state
                .slots
                .iter()
                .filter(|(_, slot)| {
                    slot.control
                        .as_ref()
                        .is_some_and(|control| control.is_cancelled())
                })
                .map(|(index, _)| *index)
                .collect::<Vec<_>>()
        };
        for index in selected {
            if !self.close_candidate(index, deadline).await? {
                return Err(failure(Stage::Close, Reason::Timeout));
            }
        }
        Ok(())
    }
    pub async fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.0.stop.cancel();
        if tokio::time::timeout_at(deadline, self.0.wait_actor_and_view())
            .await
            .is_err()
        {
            return Ok(pending("original target actor/unique view remains pending"));
        }
        let mut state = match tokio::time::timeout_at(deadline, self.0.state.lock()).await {
            Ok(state) => state,
            Err(_) => return Ok(pending("target native original is being configured")),
        };
        let indices = state.slots.keys().copied().collect::<Vec<_>>();
        let mut retired = Vec::new();
        for index in indices {
            let slot = state
                .slots
                .get_mut(&index)
                .expect("same original graph index");
            let result = if slot.raw.is_some() {
                slot.try_close(&self.0.stop)
            } else {
                slot.control
                    .as_mut()
                    .ok_or_else(|| failure(Stage::Close, Reason::Internal))?
                    .wait_closed(deadline)
                    .await
            };
            match result {
                Ok(report) => retired.extend(state.collect(index, report)),
                Err(source) => {
                    let source = state
                        .first_fault
                        .get_or_insert_with(|| Arc::new(source))
                        .clone();
                    drop(state);
                    drop(retired);
                    self.0.reported_fault.store(true, Ordering::Release);
                    return Err(retained(source));
                }
            }
        }
        if state.slots.is_empty() {
            self.0.closed.store(true, Ordering::Release);
        }
        // This actual AcceptedResource receiver retains the original fault.
        // Historical faults do not turn positive physical closure into Unknown.
        let unreported = state
            .first_fault
            .as_ref()
            .filter(|_| !self.0.reported_fault.load(Ordering::Acquire))
            .cloned();
        let report = if unreported.is_none() {
            Some(state.report())
        } else {
            None
        };
        drop(state);
        drop(retired);
        match unreported {
            Some(source) => {
                self.0.reported_fault.store(true, Ordering::Release);
                Err(retained(source))
            }
            None => Ok(report.expect("report retained across external destructor")),
        }
    }
}
impl Core {
    pub(super) fn acked(&self) -> bool {
        match self.driver.get() {
            Some(driver) => driver.try_joined().is_some_and(|joined| {
                driver.matches_joined(&joined) && joined.registration_released()
            }),
            None => {
                matches!(self.registration.load(Ordering::Acquire), 0 | 3)
                    || self.pool.scope.is_drained()
            }
        }
    }
    async fn wait_actor_and_view(&self) {
        match self.driver.get() {
            Some(driver) => {
                driver.wait().await;
            }
            None if matches!(self.registration.load(Ordering::Acquire), 0 | 3) => {}
            None => self.pool.scope.wait_drained().await,
        }
        if self.view_issued.load(Ordering::Acquire) {
            self.returned.cancelled().await;
        }
    }
    pub(super) async fn progress(self: Arc<Self>) {
        self.wait_actor_and_view().await;
        let mut state = self.state.lock().await;
        let indices = state.slots.keys().copied().collect::<Vec<_>>();
        let mut retired = Vec::new();
        for index in indices {
            match state.slots.get_mut(&index).unwrap().try_close(&self.stop) {
                Ok(report) => retired.extend(state.collect(index, report)),
                Err(source) => {
                    state.first_fault.get_or_insert_with(|| Arc::new(source));
                    drop(state);
                    drop(retired);
                    return;
                }
            }
        }
        if state.slots.is_empty() {
            self.closed.store(true, Ordering::Release);
        }
        drop(state);
        drop(retired);
    }
}
fn pending(message: &'static str) -> CloseReport {
    CloseReport {
        unfinished: vec![Unfinished::Resource(message.into())],
        ..Default::default()
    }
}
