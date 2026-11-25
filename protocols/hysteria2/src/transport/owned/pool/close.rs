use super::*;

// Current wait only. The actual receipts stay in the original task and native owners.
pub(super) type ProtocolWait = (
    &'static str,
    Option<zero_core::component::TaskId>,
    Option<zero_transport::quic::owned::OwnedDriverOrigin>,
);

#[derive(Debug)]
struct CloseTimeout {
    waiting_for: String,
    elapsed: Option<tokio::time::error::Elapsed>,
}

impl std::fmt::Display for CloseTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Hysteria2 close deadline while waiting for {}",
            self.waiting_for
        )
    }
}

impl std::error::Error for CloseTimeout {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.elapsed.as_ref().map(|elapsed| elapsed as _)
    }
}

pub(super) fn is_close_timeout(source: &ChannelError) -> bool {
    std::error::Error::source(source).is_some_and(|source| source.is::<CloseTimeout>())
}

impl Entry {
    pub(super) fn close_timeout(
        &self,
        waiting_for: impl std::fmt::Display,
        elapsed: Option<tokio::time::error::Elapsed>,
    ) -> ChannelError {
        let progress = *self.protocol_wait.lock().unwrap();
        let waiting_for = match progress {
            Some((stage, task, origin)) => {
                format!("{waiting_for}; protocol: {stage}, task {task:?}, origin {origin:?}")
            }
            None => waiting_for.to_string(),
        };
        error(Stage::Close, Reason::Timeout).with_source(CloseTimeout {
            waiting_for,
            elapsed,
        })
    }

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
        let state = tokio::time::timeout_at(end, self.state.lock())
            .await
            .map_err(|source| self.close_timeout("entry state after setup join", Some(source)))?;
        if state.closed {
            return Ok(());
        }
        drop(state);
        let retirement = self.retire_current_endpoint(end).await;
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
                .map_err(|source| {
                    self.close_timeout("uncertain setup registration Scope drain", Some(source))
                })?;
        }
        let mut state = self.state.lock().await;
        state.setup_registration = None;
        state.closed = true;
        Ok(())
    }
}
impl EntryState {
    pub(super) async fn retire_endpoint(
        &mut self,
        entry: &Entry,
        end: Instant,
    ) -> Result<(), ChannelError> {
        tokio::time::timeout_at(end, self.retire_protocol_endpoint_original(entry))
            .await
            .map_err(|source| {
                entry.close_timeout("endpoint protocol retirement", Some(source))
            })??;
        if let Some(permit) = &self.anchor_permit {
            tokio::time::timeout_at(
                end,
                permit.close_original(self.anchor_receipt.as_ref(), self.tasks.as_ref().unwrap()),
            )
            .await
            .map_err(|source| entry.close_timeout("anchor native receipt", Some(source)))??;
        }
        if let Some(permit) = &self.permit {
            tokio::time::timeout_at(
                end,
                permit.close_original(self.receipt.as_ref(), self.tasks.as_ref().unwrap()),
            )
            .await
            .map_err(|source| entry.close_timeout("current native receipt", Some(source)))??;
            // The real phase has returned protocol custody. NativeClosed and
            // the endpoint/packet registrations still come from this provider.
            self.retiring_registrations = self.receipt.clone();
        } else if self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.closed().is_none())
        {
            // A failed registration returned its preparation to the Root.
            // Keep that same original until its no-acquisition close is seen.
            return Err(error(Stage::Close, Reason::Timeout));
        }
        if let Some(permit) = &self.previous_permit {
            if permit.origin().closed().is_none() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
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
        self.protocol_origin = None;
        self.anchor_permit = None;
        self.anchor_receipt = None;
        self.previous_permit = None;
        self.retirement = None;
        self.original_stop = None;
        self.protocol_closed = false;
        self.receipt = None;
        self.unbound = None;
        self.unstarted = None;
        self.queues = None;
        self.udp_join = None;
        self.udp_exit = None;
        Ok(())
    }

    pub(super) async fn retire_protocol_endpoint_original(
        &mut self,
        entry: &Entry,
    ) -> Result<(), ChannelError> {
        if self.protocol_closed {
            return Ok(());
        }
        if let Some(auth) = &self.auth {
            auth.request_udp_stop();
        }
        if let Some(auth) = &self.pending {
            auth.request_udp_stop();
        }
        // The actual H3 owner requests cancellation before local raw close;
        // peer failures already recorded in its real ledger stay failures.
        if let Some(owner) = &self.owner {
            owner.revoke_users();
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
                *entry.protocol_wait.lock().unwrap() =
                    Some(("UDP reader join", witness.task_id(), None));
                let joined = witness.wait().await;
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
        if let Some(owner) = &self.owner {
            *entry.protocol_wait.lock().unwrap() = Some((
                "return of endpoint users before socket retirement",
                None,
                None,
            ));
            owner.retire_packet_client_original().await?;
        }
        let tasks = self
            .owner
            .as_ref()
            .map(|owner| owner.driver_tasks())
            .unwrap_or_default();
        for task in tasks {
            if !self.received.iter().any(|exit| exit.joined.id() == task) {
                let origin = self
                    .owner
                    .as_ref()
                    .and_then(|owner| owner.driver_origin(task));
                *entry.protocol_wait.lock().unwrap() =
                    Some(("endpoint driver receipt", Some(task), origin));
                let exit = self
                    .receiver
                    .as_mut()
                    .unwrap()
                    .receive_original(task)
                    .await?;
                self.received.push(exit);
            }
        }
        if let Some(receiver) = self.receiver.as_mut() {
            for delivery in &self.received {
                receiver.release_body(delivery)?;
            }
        }
        if let Some(owner) = &self.owner {
            *entry.protocol_wait.lock().unwrap() =
                Some(("endpoint users and driver joins", None, None));
            let users = owner.shutdown_users_original().await?;
            if !users.users_and_drivers_complete() {
                return Err(error(Stage::Close, Reason::Timeout));
            }
        }
        if self.permit.is_some() && self.receipt.is_none() {
            if let Some(segment) = &mut self.segment {
                *entry.protocol_wait.lock().unwrap() =
                    Some(("packet driver and returned views", None, None));
                let (queues, receipt) = segment.retire_final_original().await?;
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
        if let Some(receipt) = self.anchor_receipt.as_ref() {
            let completed = self
                .owner
                .as_ref()
                .ok_or_else(|| error(Stage::Close, Reason::Internal))?
                .complete_packet_protocol_receipt(receipt.clone())?;
            self.anchor_receipt = Some(completed);
        }
        self.protocol_closed = true;
        *entry.protocol_wait.lock().unwrap() = None;
        Ok(())
    }
}
