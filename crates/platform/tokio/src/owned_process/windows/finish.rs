use super::*;
impl OwnedProcess {
    /// Outer host cleanup only; the process actor waits only for the physical
    /// tree and then returns so its actual Scope can consume that original exit.
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
        if let Some(actor) = &state.actor {
            let Some(joined) = actor.try_joined() else {
                return Ok(None);
            };
            if !actor.matches_joined(&joined) {
                return Ok(None);
            }
        } else if !state.stop || state.attempted {
            return Ok(None);
        }
        state.progress();
        if !state.tree {
            return Ok(None);
        }
        let acquired = state
            .originals
            .iter()
            .any(|original| original.raw.is_some());
        // ActiveProcesses=0 plus this root's actual native wait precede every
        // explicit original close. Kill-on-close alone never supplies a proof.
        for index in [THREAD, PROCESS, NULL, JOB] {
            state.close(index);
        }
        if let Some(error) = state
            .originals
            .iter()
            .find_map(|original| original.unknown.clone())
        {
            return Err(io::Error::other(SharedFailure(error)));
        }
        let closed = Arc::new(ProcessTreeClosed {
            token: self.token,
            had_native_acquisition: acquired,
        });
        state.closed = Some(closed.clone());
        state.request = None;
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
