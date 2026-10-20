use super::{error, grant::CarrierGrants, slot::Slot};
use crate::contract::{
    lifecycle::{BuildError, ChannelControl, CloseReport, ResourceLifecycle, Unfinished},
    CancellationToken, CloseReason, DeliveryState, Reason, Stage,
};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::time::{timeout_at, Instant};
use zero_core::component::ComponentInstanceId;
pub(super) struct Shared {
    pub(super) instance: ComponentInstanceId,
    pub(super) limit: NonZeroUsize,
    pub(super) cancel: CancellationToken,
    pub(super) state: Mutex<State>,
    pub(super) completion: Arc<super::registration::Completion>,
}
#[derive(Default)]
pub(super) struct State {
    pub(super) live: bool,
    pub(super) stopped: bool,
    pub(super) next: u64,
    pub(super) binding: Option<Arc<super::binding::BoundAuthority>>,
    pub(super) slots: Vec<Arc<Slot>>,
    confirmed: CloseReport,
}
impl Shared {
    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}
/// Sole cleanup owner. Construction is cold; explicit lifecycle start grants
/// finite admission. Roleless pools use their actual component generation.
pub struct CarrierOwner(pub(super) Arc<Shared>);
impl CarrierOwner {
    pub fn new(
        instance: ComponentInstanceId,
        limit: NonZeroUsize,
        parent: &CancellationToken,
    ) -> Self {
        Self(Arc::new(Shared {
            instance,
            limit,
            cancel: parent.child_token(),
            state: Mutex::new(State::default()),
            completion: Arc::new(super::registration::Completion::default()),
        }))
    }
    pub fn registration(&self) -> super::CarrierOwnerRegistration {
        super::CarrierOwnerRegistration::new(&self.0)
    }
    pub fn grants(&self) -> CarrierGrants {
        CarrierGrants(self.0.clone())
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn retained(&self) -> usize {
        self.0.state().slots.len()
    }
    pub fn revoke(&self) {
        let slots = {
            let mut state = self.0.state();
            state.live = false;
            state.stopped = true;
            state.slots.clone()
        };
        self.0.cancel.cancel();
        for slot in slots {
            slot.cancel.cancel();
            slot.notify();
        }
    }
}
impl Drop for CarrierOwner {
    fn drop(&mut self) {
        self.revoke();
    }
}
impl ResourceLifecycle for CarrierOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        let bound = { self.0.state().binding.clone() };
        if bound.is_some_and(|bound| !bound.available()) {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        let mut state = self.0.state();
        if state.stopped
            || self.0.cancel.is_cancelled()
            || state
                .binding
                .as_ref()
                .is_some_and(|bound| bound.host_cancelled())
        {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::Timeout,
                DeliveryState::NotSubmitted,
            ));
        }
        state.live = true;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        CarrierOwner::revoke(self);
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke();
        let slots = self.0.state().slots.clone();
        let mut unfinished = CloseReport::default();
        let mut failure = None;
        for slot in slots {
            match close_slot(&slot, deadline).await {
                Ok(report) if report.is_complete() => {
                    self.0.state().confirmed.append(report);
                }
                Ok(report) => unfinished.append(report),
                Err(source) => {
                    if failure.is_none() {
                        failure = Some(source);
                    }
                }
            }
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        let mut report = std::mem::take(&mut self.0.state().confirmed);
        report.append(unfinished);
        if report.is_complete() {
            // Revoke preceded the exact original-slot close loop. No new
            // reservation can race this generation's explicit confirmation.
            self.0.completion.confirm();
        }
        Ok(report)
    }
}
pub(super) async fn close_slot(
    slot: &Arc<Slot>,
    deadline: Instant,
) -> Result<CloseReport, BuildError> {
    let _closing = match timeout_at(deadline, slot.closing.lock()).await {
        Ok(guard) => guard,
        Err(_) => {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Io(slot.label())],
                ..Default::default()
            })
        }
    };
    loop {
        let changed = slot.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let (waiting, driver, control, unknown, closed) = {
            let state = slot.state();
            (
                state.opening || state.spawning || (state.driver.is_none() && state.view_live),
                state.driver.clone(),
                state.control.clone(),
                state.unknown.clone(),
                state.closed,
            )
        };
        // Cleanup is the persistent local receiver even when the original
        // request was cancelled before observing its auxiliary result.
        match timeout_at(
            deadline,
            super::CarrierAdoption { slot: slot.clone() }.accept_auxiliary(),
        )
        .await
        {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(format!(
                        "{} auxiliary join",
                        slot.label()
                    ))],
                    ..Default::default()
                })
            }
        }
        if closed {
            if !super::users::finish(slot, deadline).await {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Resource(format!(
                        "{} caller/view closure remains unconfirmed",
                        slot.label()
                    ))],
                    ..Default::default()
                });
            }
            let report = slot.state().pending_close.take().unwrap_or_default();
            slot.remove();
            slot.notify();
            return Ok(report);
        }
        if waiting {
            if timeout_at(deadline, changed).await.is_err() {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Io(slot.label())],
                    ..Default::default()
                });
            }
            continue;
        }
        if let Some(source) = unknown {
            return Err(error(
                slot.instance,
                Stage::Close,
                Reason::Io,
                DeliveryState::Unknown,
            )
            .with_source(source));
        }
        if let Some((id, witness)) = driver {
            let joined = match timeout_at(deadline, witness.wait()).await {
                Ok(joined) => joined,
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Task(id)],
                        ..Default::default()
                    })
                }
            };
            if joined.id() != id {
                return Err(error(
                    slot.instance,
                    Stage::Close,
                    Reason::Internal,
                    DeliveryState::Unknown,
                ));
            }
        }
        let mut report = if let Some(control) = control {
            let mut control = match timeout_at(deadline, control.lock()).await {
                Ok(control) => control,
                Err(_) => {
                    return Ok(CloseReport {
                        unfinished: vec![Unfinished::Io(slot.label())],
                        ..Default::default()
                    })
                }
            };
            control.wait_closed(deadline).await.map_err(|mut source| {
                source.component.get_or_insert(slot.instance.component);
                source.impact = crate::contract::Impact::Component;
                source
            })?
        } else {
            CloseReport::default()
        };
        if report.is_complete() {
            report.released_resources.push(slot.label());
            {
                let mut state = slot.state();
                // Persist the original native report before waiting for external
                // callers. Cancelled cleanup never loses or fabricates that proof.
                state.closed = true;
                state.pending_close = Some(report);
            }
            slot.notify();
            continue;
        }
        return Ok(report);
    }
}
