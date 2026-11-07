//! Root receives only the actual sealed noncached job, never ordinary drivers.
use super::*;
fn actual_job(slot: &Slot) -> Option<(Arc<Job>, TaskJoinWitness)> {
    let state = slot.state();
    let preparation = state.users.as_ref()?.preparation.as_ref()?;
    let job = preparation.job.get()?;
    let driver = job.driver.get()?;
    if !job.sealed.load(Ordering::Acquire)
        || preparation.published.load(Ordering::Acquire)
        || !driver.scope_identity().same_scope(&job.scope)
        || !state.driver.as_ref().is_some_and(|(id, actual)| {
            Some(*id) == driver.task_id() && actual.same_registration(driver)
        })
    {
        return None;
    }
    Some((job.clone(), driver.clone()))
}
fn receive(
    slot: &Slot,
    job: &Job,
    driver: &TaskJoinWitness,
    joined: TaskJoined,
) -> Result<(), ChannelError> {
    let fail = || {
        error(
            slot.instance,
            Stage::Close,
            Reason::Internal,
            DeliveryState::Unknown,
        )
    };
    if !driver.matches_joined(&joined) {
        return Err(fail());
    }
    // One actual job has one finite receiver row. Store the Scope-issued result
    // (including its exact error Arc) before any callback or acceptance wake.
    let retained = job.execution_exit.get_or_init(|| joined);
    if !driver.matches_joined(retained) {
        return Err(fail());
    }
    if retained.result().is_err() {
        driver
            .acknowledge_hosted_failure(retained)
            .map_err(|source| fail().with_source(source))?;
    }
    Ok(())
}
pub(in crate::controlled_carrier) async fn accept_job_exit(
    slot: &Arc<Slot>,
) -> Result<(), ChannelError> {
    let Some((job, driver)) = actual_job(slot) else {
        return Ok(());
    };
    let joined = driver.wait().await;
    receive(slot, &job, &driver, joined)
}
pub(in crate::controlled_carrier) fn try_accept_job_exit(
    slot: &Arc<Slot>,
) -> Result<(), ChannelError> {
    let Some((job, driver)) = actual_job(slot) else {
        return Ok(());
    };
    if let Some(joined) = driver.try_joined() {
        receive(slot, &job, &driver, joined)?;
    }
    Ok(())
}
