use super::*;
use tokio::time::Instant;
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(crate) fn resource(
        &self,
    ) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle> {
        zero_platform_tokio::contract::lifecycle::erase_lifecycle(self.clone())
    }
    pub(crate) async fn finish(&self, guard: &mut SourceTasksGuard<'_, T>) {
        assert!(Arc::ptr_eq(&self.inner, &guard.owner.inner));
        self.request_abort();
        while let Some(output) = guard.join_next().await {
            self.consume(output);
        }
        self.inner.state.lock().unwrap().drained = true;
    }
    pub(super) fn consume(&self, output: Result<T, SourceJoinFailure>) {
        match output {
            Err(source) => {
                self.remember(source);
            }
            Ok(output) => {
                if let Some(source) = output.into_failure() {
                    // Expected Auxiliary query failure is delivered to its
                    // original closing receiver here. It is not a fabricated
                    // failed Scope exit or an original-native close failure.
                    tracing::warn!(error=%source, task_set=self.inner.label, "Source child output received during shutdown");
                }
            }
        }
    }
    pub(crate) async fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.request_abort();
        let joined = async {
            let _consumer = self.inner.consumer.lock().await;
            while let Some(output) = self.next_joined().await {
                self.consume(output);
            }
            self.inner.state.lock().unwrap().drained = true;
        };
        if tokio::time::timeout_at(deadline, joined).await.is_err() {
            let mut report = CloseReport::default();
            report
                .unfinished
                .push(Unfinished::Resource(self.inner.label.into()));
            return Ok(report);
        }
        let state = self.inner.state.lock().unwrap();
        if let Some(source) = &state.admission {
            return Err(crate::component::instance::retained(source.clone()));
        }
        let mut report = CloseReport::default();
        report.released_resources.push(self.inner.label.into());
        for failure in &state.failure {
            if let SourceJoinFailure::Scope {
                joined,
                source: Some(source),
                ..
            } = failure.as_ref()
            {
                report
                    .failed_tasks
                    .push(zero_platform_tokio::contract::lifecycle::TaskFailure {
                        id: joined.id(),
                        class: joined.class(),
                        error: crate::component::instance::retained(source.clone()),
                    });
            }
        }
        Ok(report)
    }
}
impl<T: TaskOutput> ResourceLifecycle for ScopedSourceTasks<T> {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        if self.inner.scope.get().is_none() || self.inner.state.lock().unwrap().closing {
            return Err(error(Reason::Revoked));
        }
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.request_abort();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.close(deadline).await
    }
}
