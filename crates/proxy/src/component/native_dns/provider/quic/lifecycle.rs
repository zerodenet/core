use super::*;
impl Lifecycle {
    pub(super) fn return_unbound(&mut self) -> std::io::Result<()> {
        if self.unbound.is_some() {
            self.permit
                .return_unbound(&mut self.unbound, &self.tasks)
                .map_err(std::io::Error::other)?;
        }
        Ok(())
    }
}
impl Query {
    pub(super) async fn failed_open(
        &self,
        opening: std::io::Error,
        end: tokio::time::Instant,
    ) -> std::io::Error {
        match self.close(end).await {
            Ok(closed) => zero_dns::native::NativeDnsQuicOpenFailure::after_close(
                opening,
                self.origin.clone(),
                closed,
            ),
            Err(cleanup) => std::io::Error::new(
                cleanup.kind(),
                super::super::custody::OpenCleanupFailure { opening, cleanup },
            ),
        }
    }
    pub(super) async fn close(
        &self,
        end: tokio::time::Instant,
    ) -> std::io::Result<MetadataPoolClosed> {
        let mut state = tokio::time::timeout_at(end, self.state.lock())
            .await
            .map_err(std::io::Error::other)?;
        if state.closed.is_none() {
            state.return_unbound()?;
            if let Some(owner) = &state.owner {
                owner.begin_shutdown();
            }
            let tasks = state
                .owner
                .as_ref()
                .map(|owner| owner.driver_tasks())
                .unwrap_or_default();
            for task in tasks {
                if state
                    .received
                    .iter()
                    .any(|delivery| delivery.joined.id() == task)
                {
                    continue;
                }
                let delivery = state
                    .receiver
                    .as_mut()
                    .expect("retained actual driver receiver")
                    .receive(task, end)
                    .await
                    .map_err(std::io::Error::other)?;
                // Each original exit/source persists before another await.
                state.received.push(delivery);
            }
            // Persist the exact deliveries above before removing only the
            // old ledger's duplicate body references. Keep all real failures.
            {
                let Lifecycle {
                    receiver, received, ..
                } = &mut *state;
                if let Some(receiver) = receiver.as_mut() {
                    for delivery in received.iter() {
                        receiver
                            .release_body(delivery)
                            .map_err(std::io::Error::other)?;
                    }
                }
            }
            if let Some(owner) = &state.owner {
                let report = owner
                    .shutdown_users(end)
                    .await
                    .map_err(std::io::Error::other)?;
                if !report.users_and_drivers_complete() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "DoQ original users or driver reception remains unconfirmed",
                    ));
                }
            }
            drop(state.owner.take());
            let receipt = state
                .completion
                .as_ref()
                .map(|completion| {
                    completion.try_owned_closed().ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "DoQ original views remain retained",
                        )
                    })
                })
                .transpose()?;
            let native = state
                .permit
                .close(receipt.as_ref(), &state.tasks)
                .map_err(std::io::Error::other)?;
            state.closed = Some(native);
            self.capacity
                .lock()
                .expect("DNS QUIC budget poisoned")
                .take();
        }
        if let Some(source) = state
            .received
            .iter()
            .find_map(|delivery| delivery.joined.result().err())
        {
            return Err(std::io::Error::other(source));
        }
        Ok(state
            .closed
            .as_ref()
            .expect("confirmed same original")
            .clone())
    }
}
