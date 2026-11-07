//! Read only this receiver's actual delivered original registration, no new
//! task consumer, completion slice, failure acceptance or native permission.
use super::*;
impl<O: DriverKind, B: Send + Sync + 'static> DriverExitReceiver<O, B> {
    pub async fn wait_registration_released(
        &self,
        delivery: &DriverDelivery<B>,
        deadline: Instant,
    ) -> Result<TaskJoined, ChannelError> {
        let stale = || ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component);
        let original = delivery
            .original
            .downcast_ref::<Record<O, B>>()
            .ok_or_else(stale)?;
        if !Weak::ptr_eq(&original.owner, &Arc::downgrade(&self.shared.state))
            || !original.delivered.load(Ordering::Acquire)
            || !original.body.lock().unwrap().returned()
        {
            return Err(stale());
        }
        let witness = original
            .registration
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, witness)| witness.clone())
            .ok_or_else(stale)?;
        let received = original
            .received
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(stale)?;
        if !witness.matches_joined(&delivery.joined) || !witness.matches_joined(&received) {
            return Err(stale());
        }
        // All record/receiver locks are released before any waiter can wake
        // the original Host. The private record token also covers real reclaim.
        let released = timeout_at(deadline, witness.wait_registration_released())
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Timeout, Impact::Component)
                    .with_source(source)
            })?;
        if !witness.matches_joined(&released) || !released.registration_released() {
            return Err(stale());
        }
        Ok(released)
    }
}
