//! Original receipt consumption is synchronous with retention, before any await.
use super::*;
impl State {
    pub(super) fn prepare(
        limit: NonZeroUsize,
        budget: Arc<TaskRegistrationBudget>,
        mains: usize,
    ) -> Result<
        (
            HostedScope,
            Vec<zero_platform_tokio::contract::scope::TaskLaunch>,
        ),
        EngineError,
    > {
        let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(limit));
        scope
            .bind_task_budget(budget)
            .map_err(std::io::Error::other)?;
        let registrar = scope.registrar();
        let mut launches = Vec::new();
        for _ in 0..mains {
            launches.push(
                registrar
                    .reserve_launch(TaskClass::Critical)
                    .map_err(|source| {
                        std::io::Error::new(std::io::ErrorKind::WouldBlock, source)
                    })?,
            );
        }
        Ok((scope, launches))
    }
    pub(super) fn activate(&mut self, mut scope: HostedScope) -> Result<(), EngineError> {
        if self.scope.is_some() || !self.retained.is_empty() {
            return Err(std::io::Error::other("old Service Scope still retained").into());
        }
        scope.enable_registration().map_err(std::io::Error::other)?;
        self.stopping = false;
        self.terminal = None;
        self.scope = Some(scope);
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn install(
        &mut self,
        limit: NonZeroUsize,
        budget: Arc<TaskRegistrationBudget>,
    ) -> Result<(), EngineError> {
        let (scope, _) = Self::prepare(limit, budget, 0)?;
        self.activate(scope)
    }
    pub(super) fn fail_transition(&mut self, source: EngineError) -> EngineError {
        let source = self
            .transition_failure
            .get_or_insert_with(|| Arc::new(failure(source)));
        std::io::Error::other(ChannelError::retained(source.clone())).into()
    }
    pub(super) fn receive(&mut self, receipt: HostedExitReceipt) -> Result<(), EngineError> {
        let result = receipt.result();
        let accepted = self.retained.observe_joined(receipt.id());
        let local = accepted.as_ref().is_some_and(|joined| {
            joined.class() == TaskClass::Auxiliary || joined.result().is_ok()
        });
        if result.is_err() && !local {
            self.failures
                .entry(receipt.id())
                .or_insert_with(|| receipt.joined().clone());
            self.stopping = true;
            self.scope.as_mut().unwrap().begin_shutdown();
        } else if !self.stopping
            && self
                .mains
                .get(&receipt.id())
                .is_some_and(|requested| !requested.load(Ordering::Acquire))
        {
            self.terminal
                .get_or_insert_with(|| receipt.joined().clone());
            self.stopping = true;
            self.scope.as_mut().unwrap().begin_shutdown();
        }
        self.mains.remove(&receipt.id());
        if let Err(source) = self
            .scope
            .as_mut()
            .expect("original Service Scope")
            .acknowledge(&receipt)
        {
            let actual = self.ack_failure.get_or_insert_with(|| {
                Arc::new(
                    ChannelError::new(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Internal,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source),
                )
            });
            return Err(std::io::Error::other(ChannelError::retained(actual.clone())).into());
        }
        if local {
            Ok(())
        } else {
            result.map_err(|source| std::io::Error::other(ChannelError::retained(source)).into())
        }
    }
    pub(super) async fn close(&mut self, deadline: Instant) -> Result<(), EngineError> {
        self.retained.revoke().await;
        let Some(scope) = self.scope.as_mut() else {
            self.retained.close(deadline).await?;
            return self.scope_failure();
        };
        self.stopping = true;
        scope.begin_shutdown();
        loop {
            let next = self.scope.as_mut().unwrap().next_receipt();
            match timeout_at(deadline, next).await {
                Ok(Some(receipt)) => {
                    let _ = self.receive(receipt);
                    if self.ack_failure.is_some() {
                        return self.retained_failure();
                    }
                }
                Ok(None) => {
                    // The original scope has joined and ACKed every task. This
                    // is deliberately not a native provider close assertion.
                    self.scope.take();
                    self.mains.clear();
                    self.retained.close(deadline).await?;
                    return self.scope_failure();
                }
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Service Scope cleanup remains unconfirmed",
                    )
                    .into())
                }
            }
        }
    }
    pub(super) fn scope_failure(&self) -> Result<(), EngineError> {
        let source = self
            .failures
            .values()
            .next()
            .and_then(|joined| joined.result().err())
            .or_else(|| self.ack_failure.clone());
        if let Some(source) = source {
            return Err(std::io::Error::other(ChannelError::retained(source)).into());
        }
        if let Some(joined) = &self.terminal {
            return Err(std::io::Error::other(format!(
                "background ingress service task {:?} exited unexpectedly",
                joined.id()
            ))
            .into());
        }
        Ok(())
    }
    pub(super) fn retained_failure(&self) -> Result<(), EngineError> {
        self.scope_failure()?;
        match &self.transition_failure {
            Some(source) => {
                Err(std::io::Error::other(ChannelError::retained(source.clone())).into())
            }
            None => Ok(()),
        }
    }
}
#[cfg(test)]
mod tests;
