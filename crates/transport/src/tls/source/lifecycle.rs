use super::*;
pub(super) struct Owner {
    pub inner: Box<dyn DynResourceLifecycle>,
    pub plans: Vec<Arc<PreparedServer>>,
    pub resources: Arc<dyn ResourceAccess>,
    pub tasks: TaskRegistrar,
    pub stop: CancellationToken,
    pub workers: Vec<TaskJoinWitness>,
    pub started: bool,
}
impl ResourceLifecycle for Owner {
    async fn start(&mut self, end: tokio::time::Instant) -> Result<(), BuildError> {
        if self.started || self.stop.is_cancelled() {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            ));
        }
        self.started = true;
        for plan in &self.plans {
            tokio::select! {biased;_=self.stop.cancelled()=>return Err(ChannelError::new(Stage::Prepare,Reason::Cancelled,Impact::Component)),result=tokio::time::timeout_at(end,plan.load_initial(self.resources.as_ref()))=>result.map_err(|e|ChannelError::new(Stage::Prepare,Reason::Timeout,Impact::Component).with_source(e))??}
        }
        for plan in &self.plans {
            if plan.task_slots() == 0 {
                continue;
            }
            let actual = plan.clone();
            let resources = self.resources.clone();
            let tasks = self.tasks.downgrade();
            let stop = self.stop.clone();
            let witness = Arc::new(std::sync::OnceLock::<TaskJoinWitness>::new());
            let driver = witness.clone();
            let (send, admitted) = tokio::sync::oneshot::channel();
            let (_,original)=self.tasks.spawn_tracked(TaskClass::Critical,async move {
                if admitted.await.is_err(){return Ok(());}
                let driver=driver.get().expect("installed original TLS worker witness");
                tokio::select! {biased;_=stop.cancelled()=>{},_=actual.refresh(resources.as_ref(),driver,&tasks,&stop)=>{}}
                Ok(())
            }).map_err(|e|ChannelError::new(Stage::Prepare,Reason::Capacity,Impact::Component).with_source(e))?;
            witness
                .set(original.clone())
                .unwrap_or_else(|_| unreachable!("one TLS worker"));
            self.workers.push(original);
            send.send(()).map_err(|_| {
                ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
            })?;
        }
        self.inner.start(end).await
    }
    fn revoke(&self, reason: zero_platform_tokio::contract::CloseReason) {
        self.stop.cancel();
        self.inner.revoke(reason);
    }
    async fn shutdown(&mut self, end: tokio::time::Instant) -> Result<CloseReport, BuildError> {
        self.stop.cancel();
        // Return the inner original report directly, without suspending with
        // consumed native receipts on this caller's stack. The actual Host
        // independently consumes every installed worker witness/Scope exit,
        // file blocking join and original stream/native close receipt.
        self.inner.shutdown(end).await
    }
}
