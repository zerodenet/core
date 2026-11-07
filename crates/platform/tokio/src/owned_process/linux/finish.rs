use super::*;
impl OwnedProcess {
    /// Outer host cleanup only: this waits for the caller's actual Scope
    /// consumer. The actor itself uses wait_tree_ended and then returns.
    pub async fn wait_closed(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<Arc<ProcessTreeClosed>>> {
        self.request_stop();
        // Derived originals settle inline in this same existing outer cleanup.
        // Actor/root observations remain independent from these native receipts.
        let endpoint_failure = match self.endpoint() {
            Some(endpoint) => endpoint.shutdown(deadline).await.err(),
            None => None,
        };
        let process_result = async {
            if let Some(closed) = self.try_finish()? {
                return Ok(Some(closed));
            }
            self.wait_tree_ended(deadline).await?;
            let actor = self.lock().actor.clone();
            if let Some(actor) = actor {
                tokio::time::timeout_at(deadline, actor.wait())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "original process actor join is pending",
                        )
                    })?;
            }
            // The first inline probe close may precede its actual actor's
            // return. Once that exact join is consumed, progress the same
            // retained originals again within the original cleanup deadline.
            if let Some(endpoint) = self.endpoint() {
                endpoint
                    .shutdown(deadline)
                    .await
                    .map_err(io::Error::other)?;
            }
            self.try_finish()
        }
        .await;
        match (endpoint_failure, process_result) {
            (Some(endpoint), Err(process)) => Err(io::Error::other(EndpointAndProcessFailure {
                endpoint,
                process,
            })),
            (Some(endpoint), Ok(_)) => Err(io::Error::other(endpoint)),
            (None, result) => result,
        }
    }

    /// Called by the outer resource owner after the exact actual actor join.
    /// A task body return/abort/observer/EOF cannot satisfy this operation.
    pub fn try_finish(&self) -> io::Result<Option<Arc<ProcessTreeClosed>>> {
        if self
            .endpoint()
            .is_some_and(|endpoint| endpoint.closed().is_none())
        {
            return Ok(None);
        }
        let mut state = self.lock();
        if let Some(closed) = &state.closed {
            return Ok(Some(closed.clone()));
        }
        if let Some(actor) = state.actor.as_ref() {
            let Some(joined) = actor.try_joined() else {
                return Ok(None);
            };
            if !actor.matches_joined(&joined) || !progress::physical(&state) {
                return Ok(None);
            }
        } else if !state.stop || state.helper.is_some() || !state.originals.is_empty() {
            // A never-started, explicitly revoked cold grant acquired neither
            // a native original nor an actor. It has no fictitious task join.
            return Ok(None);
        }
        // Removing AsyncFd deregisters the borrowed native view. It never
        // closes the raw original; those closes are explicit and recorded.
        state.control.take();
        for index in 0..state.originals.len() {
            state.close(index);
        }
        state.request = None;
        if state
            .originals
            .iter()
            .any(|fd| fd.raw.is_some() || fd.failure.is_some())
        {
            return Err(io::Error::other(
                "original process descriptor closure is unconfirmed",
            ));
        }
        let closed = Arc::new(ProcessTreeClosed {
            token: self.token,
            had_native_acquisition: !state.originals.is_empty(),
        });
        state.closed = Some(closed.clone());
        let budget = state.budget.clone();
        drop(state);
        if let Some(budget) = budget {
            budget.reconcile();
        }
        Ok(Some(closed))
    }
}

#[derive(Debug)]
struct EndpointAndProcessFailure {
    endpoint: crate::contract::ChannelError,
    process: io::Error,
}
impl std::fmt::Display for EndpointAndProcessFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "process endpoint cleanup failed ({}); actual process cleanup also failed ({})",
            self.endpoint, self.process
        )
    }
}
impl std::error::Error for EndpointAndProcessFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.endpoint)
    }
}
