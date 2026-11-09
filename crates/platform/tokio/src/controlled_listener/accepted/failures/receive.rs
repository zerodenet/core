use super::*;
impl AcceptedTaskFailureRegistration {
    pub fn matches(&self, witness: &TaskJoinWitness) -> bool {
        self.witness
            .as_ref()
            .is_some_and(|actual| actual.same_registration(witness))
            && witness.scope_identity().same_scope(&self.receiver.0.scope)
    }
    pub fn receiver(&self) -> &AcceptedTaskFailures {
        &self.receiver
    }
    /// Strongly install this same original before any local failure acceptance.
    /// A successful actual join frees only its reusable liability cell, never
    /// the still-allocated table capacity or any native/task registration.
    pub fn receive(&self, joined: &TaskJoined) -> Result<(), ChannelError> {
        if self
            .witness
            .as_ref()
            .is_none_or(|actual| !actual.matches_joined(joined))
        {
            return Err(foreign());
        }
        let failed = joined.result().is_err();
        let old: Option<TaskJoinWitness> = {
            let mut rows = self.receiver.0.rows.lock().unwrap();
            let row = rows.values.get_mut(self.key.index).ok_or_else(foreign)?;
            if !failed && (row.serial != self.key.serial || !row.reserved) {
                // This same successful actual join already released its old
                // liability cell. Never touch a newer reservation using it.
                return Ok(());
            }
            if row.serial != self.key.serial
                || !row.reserved
                || row.witness.as_ref().is_none_or(|actual| {
                    !self
                        .witness
                        .as_ref()
                        .is_some_and(|expected| actual.same_registration(expected))
                })
            {
                return Err(foreign());
            }
            if let Some(actual) = &row.failure {
                return if self
                    .witness
                    .as_ref()
                    .is_some_and(|witness| witness.matches_joined(actual))
                {
                    Ok(())
                } else {
                    Err(foreign())
                };
            }
            if failed {
                row.failure = Some(joined.clone());
                None
            } else {
                row.reserved = false;
                row.success = Some(joined.clone());
                None
            }
        };
        drop(old);
        self.receiver.0.changed.notify_waiters();
        Ok(())
    }
}
