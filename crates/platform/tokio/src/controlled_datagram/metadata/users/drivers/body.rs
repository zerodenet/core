//! One transfer of the original outcome into its actual bounded receiver.
use super::*;
pub(super) enum BodyState<B> {
    Open(Option<Arc<B>>),
    Returned(Option<Weak<B>>),
}
impl<B> BodyState<B> {
    pub(super) fn current(&self) -> Option<Arc<B>> {
        match self {
            Self::Open(value) => value.clone(),
            Self::Returned(value) => value.as_ref().and_then(Weak::upgrade),
        }
    }
    pub(super) fn returned(&self) -> bool {
        matches!(self, Self::Returned(_))
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> DriverExitReceiver<O, B> {
    /// The owning Entry/segment must persist this exact delivery before calling.
    /// Remove only the old ledger's duplicate B reference. This neither closes
    /// IO, receives a new task, releases scope credit nor acknowledges Host.
    pub fn release_body(&mut self, delivery: &DriverDelivery<B>) -> Result<(), ChannelError> {
        // This private token is the exact original Record Arc emitted only by
        // receive. It remains valid if actual ACK reclaimed the map row before
        // this owning Entry finishes its synchronous transfer; no missing-row
        // inference or equal-number task can authorize a transfer.
        let record = delivery
            .original
            .downcast_ref::<Record<O, B>>()
            .ok_or_else(|| ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component))?;
        if !Weak::ptr_eq(&record.owner, &Arc::downgrade(&self.shared.state)) {
            return Err(ChannelError::new(
                Stage::Close,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let received =
            record.received.lock().unwrap().clone().ok_or_else(|| {
                ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component)
            })?;
        let witness = record
            .registration
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, witness)| witness.clone())
            .ok_or_else(|| ChannelError::new(Stage::Close, Reason::StalePlan, Impact::Component))?;
        if !record.delivered.load(Ordering::Acquire)
            || !witness.matches_joined(&delivery.joined)
            || !witness.matches_joined(&received)
        {
            return Err(ChannelError::new(
                Stage::Close,
                Reason::StalePlan,
                Impact::Component,
            ));
        }
        let old = {
            let mut body = record.body.lock().unwrap();
            match &*body {
                BodyState::Returned(actual) => {
                    let matching = match (actual, &delivery.body) {
                        (Some(actual), Some(incoming)) => {
                            Weak::ptr_eq(actual, &Arc::downgrade(incoming))
                        }
                        (None, None) => true,
                        _ => false,
                    };
                    return if matching {
                        Ok(())
                    } else {
                        Err(ChannelError::new(
                            Stage::Close,
                            Reason::StalePlan,
                            Impact::Component,
                        ))
                    };
                }
                BodyState::Open(actual) => {
                    let matching = match (actual, &delivery.body) {
                        (Some(actual), Some(incoming)) => Arc::ptr_eq(actual, incoming),
                        (None, None) => true,
                        _ => false,
                    };
                    if !matching {
                        return Err(ChannelError::new(
                            Stage::Close,
                            Reason::StalePlan,
                            Impact::Component,
                        ));
                    }
                }
            }
            let returned = delivery.body.as_ref().map(Arc::downgrade);
            std::mem::replace(&mut *body, BodyState::Returned(returned))
        };
        // A B destructor/source waker is never run under any ledger lock.
        drop(old);
        Ok(())
    }
}
