use super::*;
impl Original {
    pub(super) fn witnesses(&self) -> Vec<crate::contract::scope::TaskJoinWitness> {
        let state = self.slot.state();
        state
            .driver
            .iter()
            .map(|(_, witness)| witness.clone())
            .chain(state.auxiliary.iter().cloned())
            .collect()
    }
    pub(super) fn released(&self) -> bool {
        // Set only by Slot::remove after matching native and user settlement,
        // plus the actual acquired caller's Host publication ACK.
        let users_confirmed = {
            let state = self.slot.state();
            match &state.users {
                Some(users) => users.ready(),
                None => state.control.is_none() && state.packet.is_none() && state.loan.is_none(),
            }
        };
        self.slot.removed.load(Ordering::Acquire)
            && self.slot.preparation_usage.snapshot()[1] == 0
            && users_confirmed
            && self.witnesses().iter().all(|witness| {
                witness.try_joined().is_some_and(|joined| {
                    witness.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
}

impl State {
    pub(super) fn charged(&self) -> usize {
        self.originals.len() + self.metadata.len() + self.bound.len() + self.source.len()
    }
}
