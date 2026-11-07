//! Actual registration credit is later than native/users closure, never a new owner.
use super::slot::Slot;
use crate::contract::scope::TaskJoinWitness;
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
pub(super) fn released(slot: &Slot) -> bool {
    caller(slot).is_none_or(|caller| {
        caller
            .try_joined()
            .is_some_and(|joined| joined.registration_released())
    })
}
pub(super) async fn wait(slot: &Arc<Slot>, deadline: tokio::time::Instant) -> bool {
    let Some(caller) = caller(slot) else {
        return true;
    };
    if released(slot) {
        return true;
    }
    tokio::time::timeout_at(deadline, caller.wait_registration_released())
        .await
        .is_ok_and(|joined| caller.matches_joined(&joined) && joined.registration_released())
}
