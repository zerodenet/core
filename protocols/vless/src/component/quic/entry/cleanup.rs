//! Root retains this one cleanup future; its observer deadline never owns it.
use super::*;
use zero_platform_tokio::contract::lifecycle::CloseReport;
impl Owners {
    pub async fn close_original(&self) -> Result<CloseReport, ChannelError> {
        self.stop.cancel();
        let mut state = self.state.lock().await;
        if state.cleanup_complete {
            return Ok(CloseReport::default());
        }
        if let Some(owner) = state.endpoint.as_ref() {
            owner.begin_shutdown();
            owner.wait_users_original().await?;
        }
        // Sealed registration has joined, so the original set is now fixed.
        // Keep every actual delivery before releasing its original body. Task
        // faults stay with the real Scope/Host receiver; no fake task is made.
        let mut previous = None;
        while let Some(id) = state
            .endpoint
            .as_ref()
            .and_then(|owner| owner.next_shutdown_driver(previous))
        {
            previous = Some(id);
            if state.received.iter().any(|same| same.joined.id() == id) {
                continue;
            }
            state
                .endpoint
                .as_ref()
                .unwrap()
                .stop_mask_driver_original(id)
                .await?;
            let delivery = state
                .receiver
                .as_mut()
                .expect("same endpoint receiver")
                .receive_original(id)
                .await?;
            state.received.push(delivery);
            let State {
                receiver, received, ..
            } = &mut *state;
            receiver
                .as_mut()
                .unwrap()
                .release_body(received.last().unwrap())?;
        }
        if state.endpoint.is_none() && matches!(state.segment, Some(Segment::Borrowed(_))) {
            let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
                unreachable!()
            };
            segment.return_unstarted_endpoint()?;
        } else if matches!(state.segment, Some(Segment::Borrowed(_))) {
            let completion = state
                .endpoint
                .as_ref()
                .and_then(|owner| owner.borrowed_users_completion())
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            let receipt = completion
                .try_owned_closed()
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
                unreachable!()
            };
            let returned = segment.return_protocol_queues_original(receipt).await?;
            state.borrowed_joined = returned.joined;
            state.queues = Some(returned.queues);
        } else if let Some(Segment::Native(segment)) = state.segment.as_mut() {
            let (queues, receipt) = segment.retire_final_original().await?;
            state
                .native_origin
                .as_ref()
                .expect("same actual native issuer")
                .retain_users_receipt(&receipt, &self.tasks)?;
            state.queues = Some(queues);
        } else if state.unbound.is_some() {
            let origin = state
                .native_origin
                .clone()
                .expect("same actual native issuer");
            origin.return_unbound(
                &mut state.unbound.as_mut().unwrap().users_claim,
                &self.tasks,
            )?;
            drop(state.unbound.take());
        }
        // Do not wait for this phase from its own borrowed adoption. Root now
        // resumes native/LoanOwner closure and the independent real Host ACKs.
        let segment = state.segment.take();
        let endpoint = state.endpoint.take();
        let receiver = state.receiver.take();
        let queues = state.queues.take();
        state.cleanup_complete = true;
        drop(state);
        drop(segment);
        drop(endpoint);
        drop(receiver);
        drop(queues);
        self.changed.notify_waiters();
        Ok(CloseReport::default())
    }
}
