//! Both demand classes consume one actual finite owner before first I/O.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
impl Outbound {
    pub(super) async fn reap(&self, context: &OpenContext) -> Result<(), ChannelError> {
        let pending = self.transfers.lock().unwrap().clone();
        for transfer in pending {
            let published = transfer
                .published
                .load(std::sync::atomic::Ordering::Acquire);
            if !transfer.opening.load(std::sync::atomic::Ordering::Acquire)
                && (!published || transfer.stop.is_cancelled())
            {
                let report = match context.deadline {
                    Some(end) => transfer.close(end).await?,
                    None => transfer.try_close()?,
                };
                if !report.is_complete() && !published {
                    return Err(crate::component::instance::invalid(
                        Reason::Capacity,
                        "previous actual taskless carrier cleanup is unfinished",
                    ));
                }
            }
        }
        if let Some(end) = context.deadline {
            self.grants.reap_returned(end).await?;
        }
        Ok(())
    }
    pub(super) fn reserve_transfer(
        &self,
        caller: Option<TaskJoinWitness>,
    ) -> Result<Arc<Transfer>, ChannelError> {
        let mut transfers = self.transfers.lock().unwrap();
        transfers.retain(|transfer| !transfer.finished());
        // Native slots can finish while the real Business task still owes its
        // host ACK. The same original record keeps that credit occupied; no
        // second native reservation may bypass the per-owner N in this window.
        if transfers.len() >= self.limit.get() {
            return Err(crate::component::instance::invalid(
                Reason::Capacity,
                "actual taskless original or caller ACK still occupies capacity",
            ));
        }
        let mut grant = self.grants.reserve()?;
        let view = if caller.is_some() {
            grant.claim_user_view()?
        } else {
            grant.claim_transferred_view()?
        };
        let transfer = Arc::new(Transfer {
            stop: grant.cancellation(),
            adoption: grant.adoption(),
            grant: Mutex::new(Some(grant)),
            view: Mutex::new(Some(view)),
            owner: tokio::sync::Mutex::new(None),
            caller,
            published: false.into(),
            opening: true.into(),
        });
        transfers.push(transfer.clone());
        Ok(transfer)
    }
}
