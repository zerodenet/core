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
        // Stop and receive the actual rotation actor and any short native
        // initializer before reading the final wire set. Their original slots
        // remain retained even if rotation stopped between two synchronous steps.
        let hopping_tasks = state
            .hopping
            .as_ref()
            .map(|hop| (hop.actor.clone(), hop.initializer.clone()));
        if let Some((actor, initializer)) = hopping_tasks {
            drop(state);
            for task in actor.into_iter().chain(initializer) {
                let joined = task.wait().await;
                if !task.matches_joined(&joined) {
                    return Err(error(Stage::Close, Reason::StalePlan));
                }
                if let Err(source) = joined.result() {
                    self.diagnostics.operation(ChannelError::retained(source));
                    task.acknowledge_hosted_failure(&joined).map_err(|source| {
                        error(Stage::Close, Reason::Internal).with_source(source)
                    })?;
                }
                let mut stored = self.state.lock().await;
                let hop = stored.hopping.as_mut().unwrap();
                if hop
                    .actor
                    .as_ref()
                    .is_some_and(|actor| actor.matches_joined(&joined))
                {
                    hop.joined = Some(joined);
                }
            }
            state = self.state.lock().await;
            if let Some(hop) = state.hopping.as_mut() {
                hop.return_pending();
            }
            if let Some(Segment::Native(segment)) = state.segment.as_mut() {
                segment.seal_paused_queues();
            }
            if let Some(queues) = state.queues.as_mut() {
                queues.seal_unserved();
            }
        }
        if let Some(tcp) = state.tcp.as_ref() {
            tcp.stop();
        }
        if let Some(driver) = state.tcp_driver.clone() {
            // The original HTTP driver owns all Hyper delegates and session
            // work. Persist its exact delivered source before local acceptance;
            // native closure and the eventual Host ACK remain the Root's gates.
            drop(state);
            let joined = driver.wait().await;
            if !driver.matches_joined(&joined) {
                return Err(error(Stage::Close, Reason::StalePlan));
            }
            {
                let mut stored = self.state.lock().await;
                if let Some(previous) = &stored.tcp_joined {
                    assert!(driver.matches_joined(previous));
                } else {
                    stored.tcp_joined = Some(joined.clone());
                }
            }
            if let Err(source) = joined.result() {
                self.diagnostics.tcp_driver(source);
                driver
                    .acknowledge_hosted_failure(&joined)
                    .map_err(|source| error(Stage::Close, Reason::Internal).with_source(source))?;
            }
            state = self.state.lock().await;
        }
        if let Some(owner) = state.mkcp.as_mut() {
            owner.retire_protocol_original().await?;
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
        if state.endpoint.is_none()
            && state.mkcp.is_none()
            && matches!(state.segment, Some(Segment::Borrowed(_)))
        {
            let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
                unreachable!()
            };
            segment.return_unstarted_endpoint()?;
        } else if matches!(state.segment, Some(Segment::Borrowed(_))) {
            let completion = state
                .endpoint
                .as_ref()
                .and_then(|owner| owner.borrowed_users_completion());
            let receipt = if let Some(owner) = state.mkcp.as_ref() {
                owner.borrowed_users_closed()
            } else {
                completion.and_then(|completion| completion.try_owned_closed())
            }
            .ok_or_else(|| error(Stage::Close, Reason::Internal))?;
            let Some(Segment::Borrowed(segment)) = state.segment.as_mut() else {
                unreachable!()
            };
            let returned = segment.return_protocol_queues_original(receipt).await?;
            state.borrowed_joined = returned.joined;
            state.queues = Some(returned.queues);
        } else if state.native_users.is_none() && matches!(state.segment, Some(Segment::Native(_)))
        {
            let Some(Segment::Native(segment)) = state.segment.as_mut() else {
                unreachable!()
            };
            let (queues, receipt) = segment.retire_final_original().await?;
            state.queues = Some(queues);
            state.native_users = Some(receipt);
        } else if state.unbound.is_some() {
            let origin = state
                .native_origin
                .clone()
                .expect("same actual native issuer");
            origin
                .return_unbound_original(
                    &mut state.unbound.as_mut().unwrap().users_claim,
                    &self.tasks,
                )
                .await?;
            drop(state.unbound.take());
        }
        if let Some(receipt) = state.native_users.as_ref() {
            let original = state
                .native_origin
                .as_ref()
                .expect("same native wire issuer");
            if original
                .close_native_wire_original(receipt, &self.tasks)
                .await?
                .is_none()
            {
                return Ok(CloseReport {
                    unfinished: vec![
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "same QUIC native wire retains original close debt".into(),
                        ),
                    ],
                    ..Default::default()
                });
            }
        }
        if let Some(hop) = state.hopping.as_mut() {
            hop.close_previous(&self.tasks).await?;
        }
        // Do not wait for this phase from its own borrowed adoption. Root now
        // resumes native/LoanOwner closure and the independent real Host ACKs.
        let tcp_masks = state.tcp_masks.take();
        let tcp_setup = state.tcp_setup.take();
        let tcp = state.tcp.take();
        let segment = state.segment.take();
        let endpoint = state.endpoint.take();
        let mkcp = state.mkcp.take();
        let receiver = state.receiver.take();
        let queues = state.queues.take();

        drop(state);
        drop(tcp_masks);
        drop(tcp_setup);
        drop(tcp);
        drop(segment);
        drop(endpoint);
        drop(mkcp);
        drop(receiver);
        drop(queues);
        // Installed loans are now owned by their exact retained child Root.
        // Returning that child is separate from closing abandoned setup donors.
        self.release_installed_carriers();
        if let Some(children) = self.protocols.get() {
            children.wait_returned_original().await?;
        }
        let pending = self.close_pending_carriers()?;
        if !pending.is_complete() {
            return Ok(pending);
        }
        self.state.lock().await.cleanup_complete = true;
        self.changed.notify_waiters();
        Ok(pending)
    }
}
