//! Exact physical receipts survive cancellation until a supervisor consumes them.
use super::*;
impl InboundOwnerRegistry {
    pub(crate) async fn confirm_shutdown(
        &self,
        tag: &str,
        deadline: Instant,
    ) -> Result<Option<ClosedInboundReceipt>, EngineError> {
        let Some(owner) = self.owner(tag) else {
            return Ok(None);
        };
        let result = owner.shutdown(deadline).await;
        let terminal = match owner.terminal() {
            Some(terminal) => terminal,
            None => {
                return Err(result.err().unwrap_or_else(|| {
                    std::io::Error::other("inbound physical shutdown remains unconfirmed").into()
                }))
            }
        };
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !entries
            .get(tag)
            .is_some_and(|actual| Arc::ptr_eq(actual, &owner))
        {
            return Err(
                std::io::Error::other("inbound shutdown belongs to a retired owner").into(),
            );
        }
        let mut pending = self
            .terminals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let terminal = pending.entry(tag.to_owned()).or_insert(terminal).clone();
        Ok(Some(ClosedInboundReceipt {
            tag: tag.to_owned(),
            terminal,
        }))
    }
    /// Publication is synchronous. A failure/panic leaves the same owner and
    /// original evidence pending; a replay cannot publish again after release.
    pub(crate) fn release_closed<T>(
        &self,
        receipt: ClosedInboundReceipt,
        publish: impl FnOnce(&InboundTerminal) -> Result<T, EngineError>,
    ) -> Result<T, EngineError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut pending = self
            .terminals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let matches = pending
            .get(&receipt.tag)
            .is_some_and(|actual| Arc::ptr_eq(actual, &receipt.terminal))
            && entries
                .get(&receipt.tag)
                .is_some_and(|owner| owner.identity() == receipt.terminal.identity());
        if !matches {
            return Err(std::io::Error::other("stale or consumed inbound shutdown receipt").into());
        }
        let result = publish(&receipt.terminal)?;
        receipt.terminal.publish();
        pending.remove(&receipt.tag);
        entries.remove(&receipt.tag);
        Ok(result)
    }
    pub(crate) async fn shutdown_all_reported(
        &self,
        deadline: Instant,
        mut publish: impl FnMut(&str, &InboundTerminal) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        // Fence promotion/reservation before either owner snapshot. A late
        // native owner must remain in a persistent map until explicit close.
        self.close_bind_admission();
        let tags = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut failure = self
            .retire_bind_reservations_reported(deadline, &mut publish)
            .await
            .err();
        for tag in tags {
            let result = async {
                let Some(receipt) = self.confirm_shutdown(&tag, deadline).await? else {
                    return Ok(());
                };
                let fault = self.release_closed(receipt, |terminal| {
                    publish(&tag, terminal)?;
                    Ok(terminal.failure())
                })?;
                fault.map_or(Ok(()), |source| {
                    Err(std::io::Error::other(crate::component::instance::retained(source)).into())
                })
            }
            .await;
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
