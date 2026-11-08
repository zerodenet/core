use super::*;
impl Entry {
    pub(super) async fn close_if_idle(&self, end: Instant) -> Result<bool, ChannelError> {
        let Ok(state) = self.state.try_lock() else {
            return Ok(false);
        };
        if !state.idle() {
            return Ok(false);
        }
        // Reserve retirement under the same admission lock. The idle hint is
        // never a user/task/native receipt; close below still collects each.
        self.cancel.cancel();
        drop(state);
        self.close(end).await?;
        Ok(true)
    }
    pub(super) async fn close(&self, end: Instant) -> Result<(), ChannelError> {
        self.cancel.cancel();
        // The real setup owns this mutex during IO. Cancellation lets it store
        // its result and release the mutex; never wait its join under that lock.
        self.receive_setup(end).await?;
        let mut state = tokio::time::timeout_at(end, self.state.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        if state.closed {
            return Ok(());
        }
        let retirement = state.retire_endpoint(end).await;
        drop(state);
        self.receive_native_failure().await?;
        retirement?;
        let state = self.state.lock().await;
        let uncertain = state.setup_registration.clone();
        drop(state);
        self.wait_endpoint_ack(end).await?;
        self.wait_setup_ack(end).await?;
        if let Some(scope) = uncertain {
            tokio::time::timeout_at(end, scope.wait_drained())
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        }
        let mut state = self.state.lock().await;
        state.setup_registration = None;
        state.closed = true;
        Ok(())
    }
}
impl EntryState {
    pub(super) async fn retire_endpoint(&mut self, end: Instant) -> Result<(), ChannelError> {
        if let Some(auth) = &self.auth {
            auth.request_udp_stop();
        }
        if let Some(auth) = &self.pending {
            auth.request_udp_stop();
        }
        // The actual H3 owner requests cancellation before local raw close;
        // peer failures already recorded in its real ledger stay failures.
        if let Some(owner) = &self.owner {
            owner.begin_shutdown();
        }
        if let Some(auth) = &self.auth {
            auth.stop();
            self.udp_join = auth.udp_witness().await?;
        }
        if let Some(auth) = &self.pending {
            auth.stop();
        }
        if self.udp_exit.is_none() {
            if let Some(witness) = &self.udp_join {
                let joined = tokio::time::timeout_at(end, witness.wait())
                    .await
                    .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
                if joined.result().is_err() {
                    witness
                        .acknowledge_hosted_failure(&joined)
                        .map_err(|source| {
                            error(Stage::Close, Reason::Internal).with_source(source)
                        })?;
                }
                self.udp_exit = Some(joined);
            }
        }
        drop(self.auth.take());
        drop(self.pending.take());
        let tasks = self
            .owner
            .as_ref()
            .map(|owner| owner.driver_tasks())
            .unwrap_or_default();
        for task in tasks {
            if !self.received.iter().any(|exit| exit.joined.id() == task) {
                let exit = self.receiver.as_mut().unwrap().receive(task, end).await?;
                self.received.push(exit);
            }
        }
        if let Some(receiver) = self.receiver.as_mut() {
            for delivery in &self.received {
                receiver.release_body(delivery)?;
            }
        }
        if let Some(owner) = &self.owner {
            let users = owner.shutdown_users(end).await?;
            if !users.users_and_drivers_complete() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
        }
        if self.permit.is_some() && self.receipt.is_none() {
            if let Some(segment) = &mut self.segment {
                let (queues, receipt) = segment.retire_final(end).await?;
                self.queues = Some(queues);
                self.receipt = Some(receipt);
            } else {
                if let Some(grant) = self.unbound.take() {
                    match grant.return_unstarted() {
                        Ok(completion) => self.unstarted = Some(completion),
                        Err(grant) => {
                            self.unbound = Some(grant);
                            return Err(error(Stage::Close, Reason::Internal));
                        }
                    }
                }
                if let Some(unstarted) = &self.unstarted {
                    self.receipt = Some(
                        unstarted
                            .try_closed()
                            .ok_or_else(|| error(Stage::Close, Reason::Timeout))?,
                    );
                }
            }
        }
        if let Some(permit) = &self.permit {
            permit.close(self.receipt.as_ref(), self.tasks.as_ref().unwrap())?;
            // NativeClosed is already visible; preserve these exact real
            // endpoint/packet registrations until outside-lock Host ACK wait.
            self.retiring_registrations = self.receipt.clone();
        }
        if let Some(segment) = &self.segment {
            if let Some(exit) = segment.task_exit() {
                if exit.joined.result().is_err() {
                    self.retired_application.push(exit.joined.clone());
                }
            }
        }
        if let Some(exit) = &self.udp_exit {
            if exit.result().is_err() {
                self.retired_application.push(exit.clone());
            }
        }
        // Every original user/driver/native gate above has completed. Success
        // records are settled facts, not lifetime task history. Failed sources
        // remain actual owned debt until the host publication consumes them.
        self.retired_drivers.extend(
            self.received
                .drain(..)
                .filter(|delivery| delivery.joined.result().is_err()),
        );
        drop(self.owner.take());
        self.receiver = None;
        self.segment = None;
        self.permit = None;
        self.origin = None;
        self.receipt = None;
        self.unbound = None;
        self.unstarted = None;
        self.queues = None;
        self.udp_join = None;
        self.udp_exit = None;
        Ok(())
    }
}
