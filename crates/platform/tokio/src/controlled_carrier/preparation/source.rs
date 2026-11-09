use super::*;
impl CarrierGrant {
    /// Cached owning factories can stage their actual original driver. Normal
    /// cache publication and rejected noncached jobs are different one-way ends.
    pub fn claim_preparing_driver_view(
        &mut self,
    ) -> Result<(CarrierUserView, CarrierPreparation), ChannelError> {
        let view = self.claim_driver_view()?;
        let state = Arc::new(Preparation {
            expected_caller: None,
            auxiliary: OnceLock::new(),
            published: AtomicBool::new(false),
            connect_rejection: OnceLock::new(),
            pending: OnceLock::new(),
            job: OnceLock::new(),
        });
        self.slot
            .state()
            .users
            .as_mut()
            .expect("unique driver view installed")
            .preparation = Some(state.clone());
        Ok((
            view,
            CarrierPreparation {
                slot: Arc::downgrade(&self.slot),
                state,
            },
        ))
    }
}
impl CarrierPreparation {
    pub(in crate::controlled_carrier) fn matches_transferred_origin(
        &self,
        slot: &Arc<Slot>,
    ) -> bool {
        self.slot
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, slot))
            && self.state.expected_caller.is_none()
            && !self.is_published()
            && self
                .state
                .auxiliary
                .get()
                .is_some_and(|actual| !actual.is_joined())
            && slot.state().users.as_ref().is_some_and(|users| {
                users.live()
                    && users.acquired()
                    && users.transferred()
                    && users
                        .preparation
                        .as_ref()
                        .is_some_and(|p| Arc::ptr_eq(p, &self.state))
            })
    }
}
impl crate::controlled_carrier::CarrierIo {
    pub fn preparation(&self) -> Option<CarrierPreparation> {
        let state = self.slot.state();
        Some(CarrierPreparation {
            slot: Arc::downgrade(&self.slot),
            state: state.users.as_ref()?.preparation.clone()?,
        })
    }
}

impl crate::controlled_carrier::CarrierAdoption {
    pub fn preparation(&self) -> Option<CarrierPreparation> {
        let state = self.slot.state();
        Some(CarrierPreparation {
            slot: Arc::downgrade(&self.slot),
            state: state.users.as_ref()?.preparation.clone()?,
        })
    }
}
