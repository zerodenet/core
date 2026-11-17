use super::*;
impl OwnedProcess {
    /// Outer host cleanup only: this waits for the caller's actual Scope
    /// consumer. The actor itself uses wait_child_ended and then returns.
    pub async fn wait_closed(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<Arc<ProcessChildClosed>>> {
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
            self.wait_child_ended(deadline).await?;
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
    pub fn try_finish(&self) -> io::Result<Option<Arc<ProcessChildClosed>>> {
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
            if !actor.matches_joined(&joined) || !state.physical() {
                return Ok(None);
            }
        } else if !state.stop || state.acquired {
            // A never-started, explicitly revoked cold grant acquired neither
            // a native original nor an actor. It has no fictitious task join.
            return Ok(None);
        }
        // Actual wait status precedes releasing this exact Child handle. Its
        // Drop is never used to claim termination, and no tree proof is issued.
        state.child.take();
        state.request = None;
        let closed = Arc::new(ProcessChildClosed {
            token: self.token,
            had_native_acquisition: state.acquired,
            status: state.root_status,
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
