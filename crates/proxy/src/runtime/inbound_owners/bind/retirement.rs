//! Cancellation retains the actual reservation until proof and source handoff.
use super::*;

impl InboundOwnerRegistry {
    pub(crate) async fn retire_bind_reservation_reported(
        &self,
        tag: &str,
        deadline: Instant,
        publish: impl FnOnce(&InboundTerminal) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let owner = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(tag)
            .cloned();
        let Some(owner) = owner else {
            return Ok(());
        };
        let result = owner.shutdown(deadline).await;
        let Some(terminal) = owner.terminal() else {
            return Err(result
                .err()
                .unwrap_or_else(|| io("native bind reservation remains unconfirmed")));
        };
        let mut reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !reserved
            .entries
            .get(tag)
            .is_some_and(|actual| Arc::ptr_eq(actual, &owner))
        {
            return Err(io(
                "native bind reservation was already consumed or promoted",
            ));
        }
        // Same synchronous publication rule as active listeners: failure or
        // panic preserves this exact owner and its light original terminal.
        publish(&terminal)?;
        terminal.publish();
        reserved.entries.remove(tag);
        if reserved.entries.is_empty() {
            reserved.config = Weak::new();
        }
        super::super::released_fault::reported_fault(&terminal).map_or(Ok(()), Err)
    }

    pub(crate) async fn retire_bind_reservations_reported(
        &self,
        deadline: Instant,
        mut publish: impl FnMut(&str, &InboundTerminal) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let owners = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .iter()
            .map(|(tag, owner)| (tag.clone(), owner.clone()))
            .collect::<Vec<_>>();
        for (_, owner) in &owners {
            owner.task_access().request_stop();
        }
        let mut failure = None;
        for (tag, _) in owners {
            if let Err(error) = self
                .retire_bind_reservation_reported(&tag, deadline, |terminal| {
                    publish(&tag, terminal)
                })
                .await
            {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
