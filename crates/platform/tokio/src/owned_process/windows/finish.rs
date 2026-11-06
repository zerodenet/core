use super::*;
impl OwnedProcess {
    /// Outer host cleanup only; the process actor waits only for the physical
    /// tree and then returns so its actual Scope can consume that original exit.
    pub async fn wait_closed(
        &self,
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<Arc<ProcessTreeClosed>>> {
        self.request_stop();
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
    pub fn try_finish(&self) -> io::Result<Option<Arc<ProcessTreeClosed>>> {
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
