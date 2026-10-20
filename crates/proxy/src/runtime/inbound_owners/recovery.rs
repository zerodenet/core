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
        pending.remove(&receipt.tag);
        entries.remove(&receipt.tag);
        Ok(result)
    }
}
