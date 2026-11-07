//! The same finite actual control-budget transaction.
use super::*;
impl ControlBudgetCandidate {
    pub fn limits(&self) -> ControlBudgetLimits {
        self.limits
    }
    fn finish(&self, commit: bool) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original root control owner expired"))?;
        let mut state = owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state
            .pending
            .as_ref()
            .ok_or_else(|| io::Error::other("root control candidate was already settled"))?;
        if !Arc::ptr_eq(&pending.identity, &self.identity) || pending.limits != self.limits {
            return Err(io::Error::other("foreign root control candidate"));
        }
        if !commit && pending.committed {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "committed control candidate awaits actual publication",
            ));
        }
        if pending
            .scope_pause
            .as_ref()
            .is_some_and(|pause| !pause.complete() && (commit || pause.started()))
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "original raw command Scope has not retired",
            ));
        }
        let scope_pause = pending.scope_pause.clone();
        if commit {
            state.limits = self.limits;
        }
        state.pending = None;
        drop(state);
        if let Some(pause) = scope_pause {
            pause.release();
        }
        owner.1.notify_waiters();
        Ok(())
    }
    pub async fn retire_command_scope(&self, end: tokio::time::Instant) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let pause = {
            let state = owner
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = state
                .pending
                .as_ref()
                .filter(|pending| Arc::ptr_eq(&pending.identity, &self.identity))
                .ok_or_else(|| io::Error::other("foreign control budget attempt"))?;
            pending.scope_pause.clone()
        };
        if let Some(pause) = pause {
            pause.retire(end).await.map_err(io::Error::other)?;
        }
        Ok(())
    }
    pub async fn finish_original_command_retirement(&self) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let pause = {
            let state = owner
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = state
                .pending
                .as_ref()
                .filter(|p| Arc::ptr_eq(&p.identity, &self.identity))
                .ok_or_else(|| io::Error::other("foreign control budget attempt"))?;
            pending.scope_pause.clone()
        };
        if let Some(pause) = pause {
            pause.finish_original().await.map_err(io::Error::other)?;
        }
        Ok(())
    }
    pub fn is_pending(&self) -> io::Result<bool> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let state = owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &state.pending {
            Some(pending) if Arc::ptr_eq(&pending.identity, &self.identity) => Ok(true),
            Some(_) => Err(io::Error::other("foreign control budget attempt")),
            None => Ok(false),
        }
    }
    /// Pure metadata settlement. The original pending row retains the release
    /// duty even if its accepted operation unwinds before final publication.
    pub fn commit_deferred(&self) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let mut state = owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state
            .pending
            .as_ref()
            .filter(|p| Arc::ptr_eq(&p.identity, &self.identity) && p.limits == self.limits)
            .ok_or_else(|| io::Error::other("foreign control budget publication"))?;
        if pending
            .scope_pause
            .as_ref()
            .is_some_and(|pause| !pause.complete())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "original raw Scope remains retained",
            ));
        }
        state.limits = self.limits;
        state.pending.as_mut().unwrap().committed = true;
        Ok(())
    }
    pub fn metadata_committed(&self) -> io::Result<bool> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let state = owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &state.pending {
            Some(pending) if Arc::ptr_eq(&pending.identity, &self.identity) => {
                Ok(pending.committed)
            }
            Some(_) => Err(io::Error::other("foreign control publication")),
            None => Ok(false),
        }
    }
    /// Caller has dropped every Engine/query book guard before invoking any
    /// potentially reentrant wake or restarting the same receipt duty.
    pub fn publish_committed(&self) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original control book expired"))?;
        let pause = {
            let mut state = owner
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let pending = state
                .pending
                .as_ref()
                .filter(|p| Arc::ptr_eq(&p.identity, &self.identity) && p.committed)
                .ok_or_else(|| io::Error::other("control metadata publication is not committed"))?;
            let pause = pending.scope_pause.clone();
            state.pending = None;
            pause
        };
        if let Some(pause) = pause {
            pause.release();
        }
        owner.1.notify_waiters();
        Ok(())
    }
    pub fn commit(&self) -> io::Result<()> {
        self.commit_deferred()?;
        self.publish_committed()
    }
    pub fn retire(&self) -> io::Result<()> {
        self.finish(false)
    }
}

impl ControlBudget {
    /// Borrow the exact retained token, including after an accepted operation
    /// panics. This creates no permission, Scope, book or native resource.
    pub fn retained_candidate(self: &Arc<Self>) -> Option<ControlBudgetCandidate> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state.pending.as_ref()?;
        Some(ControlBudgetCandidate {
            owner: Arc::downgrade(self),
            identity: pending.identity.clone(),
            limits: pending.limits,
        })
    }
    pub fn candidate_for_publication(
        self: &Arc<Self>,
        limits: ControlBudgetLimits,
    ) -> io::Result<Option<ControlBudgetCandidate>> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(pending) = state.pending.as_ref().filter(|p| p.limits == limits) {
            return Ok(Some(ControlBudgetCandidate {
                owner: Arc::downgrade(self),
                identity: pending.identity.clone(),
                limits,
            }));
        }
        if state.limits == limits {
            return Ok(None);
        }
        Err(io::Error::other(
            "configuration has no matching original control budget candidate",
        ))
    }
    /// External shutdown has already joined the real command owner. A dropped
    /// operation's token must not make that same Host permanently unreopenable.
    pub async fn retire_candidate_after_shutdown(self: &Arc<Self>) -> io::Result<()> {
        {
            let state = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.closed || !self.2.retirement_complete() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "original command owner has not retired",
                ));
            }
        }
        if let Some(candidate) = self.retained_candidate() {
            let cleanup = candidate.finish_original_command_retirement().await;
            let committed = {
                let state = self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.pending.as_ref().is_some_and(|p| p.committed)
            };
            let settled = if committed {
                candidate.publish_committed()
            } else {
                candidate.retire()
            };
            // The original owner separately retains every typed cleanup fault;
            // a failed settlement keeps the exact pending token in that book.
            settled?;
            cleanup?;
        }
        Ok(())
    }
}
