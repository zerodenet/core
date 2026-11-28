//! Both demand classes consume one actual finite owner before first I/O.
use super::*;
use zero_platform_tokio::contract::scope::TaskJoinWitness;
impl Outbound {
    pub(super) async fn reap(&self, context: &OpenContext) -> Result<(), ChannelError> {
        // One grace budget for the whole reap, not five seconds per old slot.
        let brief = Instant::now() + std::time::Duration::from_secs(5);
        let pending = self.transfers.lock().unwrap().clone();
        for transfer in pending {
            let published = transfer
                .published
                .load(std::sync::atomic::Ordering::Acquire);
            if !transfer.opening.load(std::sync::atomic::Ordering::Acquire)
                && transfer.adoption.retained_preparation_job().is_none()
                && (!published || transfer.stop.is_cancelled())
            {
                let report = match context.deadline {
                    Some(end) => transfer.close(end).await?,
                    None => {
                        let mut report = transfer.try_close()?;
                        let caller_done = transfer
                            .caller
                            .as_ref()
                            .is_none_or(|caller| caller.is_joined());
                        if !report.is_complete()
                            && transfer.has_incomplete_cleanup_segments()
                            && (!published || (transfer.stop.is_cancelled() && caller_done))
                        {
                            report = transfer.close_with_grace(&context.cancel, brief).await?;
                        }
                        report
                    }
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
        } else {
            self.grants.try_reap_returned()?;
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
            let mut opening = 0usize;
            let mut active_published = 0usize;
            let mut closing_native = 0usize;
            let mut awaiting_caller_ack = 0usize;
            for transfer in transfers.iter() {
                if transfer.opening.load(std::sync::atomic::Ordering::Acquire) {
                    opening += 1;
                } else if transfer.adoption.closed().is_some() {
                    awaiting_caller_ack += 1;
                } else if transfer
                    .published
                    .load(std::sync::atomic::Ordering::Acquire)
                    && !transfer.stop.is_cancelled()
                {
                    active_published += 1;
                } else {
                    closing_native += 1;
                }
            }
            return Err(crate::component::instance::invalid(
                Reason::Capacity,
                format!(
                    "actual taskless original or caller ACK still occupies capacity (limit={}, in_use={}, active_published={}, opening={}, closing_native={}, awaiting_caller_ack={})",
                    self.limit.get(),
                    transfers.len(),
                    active_published,
                    opening,
                    closing_native,
                    awaiting_caller_ack,
                ),
            ));
        }
        let mut grant = self.grants.reserve()?;
        let (view, preparation) = match (&caller, &self.tasks) {
            (Some(caller), Some(_)) => {
                let (view, preparation) = grant.claim_preparing_user_view(caller)?;
                (view, Some(preparation))
            }
            (Some(_), None) => (grant.claim_user_view()?, None),
            (None, Some(_)) => {
                let (view, preparation) = grant.claim_preparing_transferred_view()?;
                (view, Some(preparation))
            }
            (None, None) => (grant.claim_transferred_view()?, None),
        };
        let transfer = Arc::new(Transfer {
            driver_input: Mutex::new(None),
            stop: grant.cancellation(),
            adoption: grant.adoption(),
            grant: Mutex::new(Some(grant)),
            view: Mutex::new(Some(view)),
            owner: Arc::new(tokio::sync::Mutex::new(None)),
            caller,
            preparation,
            pending: Mutex::new(Vec::new()),
            preparing: Mutex::new(None),
            cleanup: Default::default(),
            cleanup_network: self.cleanup_network.clone(),
            retained_prefix: Default::default(),
            published: false.into(),
            opening: true.into(),
        });
        transfers.push(transfer.clone());
        Ok(transfer)
    }
}
