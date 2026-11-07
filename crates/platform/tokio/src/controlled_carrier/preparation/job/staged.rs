//! One original's actual retained IO; no observer can steal another Scope's job.
use super::*;
impl crate::controlled_carrier::CarrierAdoption {
    pub fn retained_preparation_job(&self) -> Option<CarrierPreparationJobRetained> {
        let job = self
            .slot
            .state()
            .users
            .as_ref()?
            .preparation
            .as_ref()?
            .job
            .get()?
            .clone();
        let proof = CarrierPreparationJobRetained {
            slot: Arc::downgrade(&self.slot),
            job,
        };
        proof.retains(self).then_some(proof)
    }
    pub fn take_preparation_job(
        &self,
        tasks: &TaskRegistrar,
        received: &TaskJoined,
    ) -> Result<Option<StagedCarrierPreparationJob>, ChannelError> {
        let preparation = self
            .slot
            .state()
            .users
            .as_ref()
            .and_then(|users| users.preparation.clone());
        let Some(preparation) = preparation else {
            return Ok(None);
        };
        let Some(job) = preparation.job.get() else {
            return Ok(None);
        };
        let auxiliary = preparation.auxiliary.get().ok_or_else(revoked)?;
        if !job.scope.matches(tasks)
            || !auxiliary.matches_joined(received)
            || !received.registration_released()
            || received.result().is_ok()
            || !received.failure_accepted()
            || job.sealed.load(Ordering::Acquire)
        {
            return Err(revoked());
        }
        let staged = job.staged.lock().unwrap().take();
        Ok(staged.map(|staged| StagedCarrierPreparationJob {
            grant: CarrierPreparationJob {
                slot: Arc::downgrade(&self.slot),
                preparation: preparation.clone(),
                job: job.clone(),
            },
            return_delay: staged.delay,
            work: staged.work,
        }))
    }
}
pub(in crate::controlled_carrier) fn cancel_staged(slot: &Arc<Slot>) {
    if !slot.cancel.is_cancelled() {
        return;
    }
    let job = slot
        .state()
        .users
        .as_ref()
        .and_then(|users| users.preparation.as_ref())
        .and_then(|preparation| preparation.job.get())
        .cloned();
    let work = job.and_then(|job| {
        if job.sealed.load(Ordering::Acquire) {
            return None;
        }
        let pending = job.staged.lock().unwrap().take();
        pending
    });
    // A real IO destructor can wake/reenter its original caller. Never drop it
    // under any Slot, root-accounting or job metadata lock.
    drop(work);
}
