//! Native/users closure is visible before this same original's ACK refund.
use super::slot::Slot;
use crate::contract::{scope::TaskJoinWitness, BoxFuture};
use std::sync::Arc;
pub(super) fn caller(slot: &Slot) -> Option<TaskJoinWitness> {
    let state = slot.state();
    let users = state.users.as_ref()?;
    if users
        .preparation
        .as_ref()
        .is_some_and(|preparation| preparation.job.get().is_some())
    {
        if let Some((_, driver)) = &state.driver {
            // This actual Slot registered the job, even if its later seal
            // failed. Native/UsersClosed remain independently observable;
            // local N removal alone waits for this actual registration ACK.
            return Some(driver.clone());
        }
    }
    users.credit_caller()
}

#[derive(Default)]
struct Registrations {
    caller: Option<TaskJoinWitness>,
    auxiliary: Option<TaskJoinWitness>,
    driver: Option<TaskJoinWitness>,
}
impl Registrations {
    fn snapshot(slot: &Slot) -> Self {
        let caller = caller(slot);
        let state = slot.state();
        // Unlike credit_caller, these exact native/driver registrations exist
        // even before users are installed or a logical loan is published.
        Self {
            caller,
            auxiliary: state.auxiliary.clone(),
            driver: state.driver.as_ref().map(|(_, actual)| actual.clone()),
        }
    }
    fn present(&self) -> bool {
        self.caller.is_some() || self.auxiliary.is_some() || self.driver.is_some()
    }
    fn released(&self) -> bool {
        self.caller
            .iter()
            .chain(self.auxiliary.iter())
            .chain(self.driver.iter())
            .all(|actual| {
                actual.try_joined().is_some_and(|joined| {
                    actual.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
    async fn wait(self) -> bool {
        for actual in self
            .caller
            .into_iter()
            .chain(self.auxiliary)
            .chain(self.driver)
        {
            let joined = actual.wait_registration_released().await;
            if !actual.matches_joined(&joined) || !joined.registration_released() {
                return false;
            }
        }
        true
    }
}
struct Credits {
    primary: Registrations,
    related: Option<Registrations>,
}
fn credits(slot: &Slot) -> Credits {
    let primary = Registrations::snapshot(slot);
    let related = { slot.state().related_child.clone() };
    // Follow only the actual already-linked child. The child's cleanup must
    // not wait its parent control driver: control closes after datagram views,
    // and reversing this dependency would wait for its own close continuation.
    let related = related.as_ref().map(|child| Registrations::snapshot(child));
    Credits { primary, related }
}
impl Credits {
    fn released(&self) -> bool {
        self.primary.released() && self.related.as_ref().is_none_or(Registrations::released)
    }
    fn present(&self) -> bool {
        self.primary.present() || self.related.as_ref().is_some_and(Registrations::present)
    }
    async fn wait(self) -> bool {
        if !self.primary.wait().await {
            return false;
        }
        match self.related {
            Some(related) => related.wait().await,
            None => true,
        }
    }
}
pub(super) fn released(slot: &Slot) -> bool {
    credits(slot).released()
}
/// One actual immutable witness observation in the original retirement row.
/// No Task/Scope, exit acceptance, native close or new deadline is created.
pub(super) fn observe(slot: &Slot) -> Option<BoxFuture<'static, bool>> {
    let actual = credits(slot);
    actual
        .present()
        .then(|| Box::pin(actual.wait()) as BoxFuture<'static, bool>)
}
pub(super) async fn wait(slot: &Arc<Slot>, deadline: tokio::time::Instant) -> bool {
    if released(slot) {
        return true;
    }
    let actual = credits(slot);
    tokio::time::timeout_at(deadline, actual.wait())
        .await
        .is_ok_and(|actual| actual)
        && released(slot)
}
