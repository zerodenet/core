use super::*;

impl EventDispatcherHandle {
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_retained().await;
    }

    pub async fn shutdown_retained(&mut self) -> ConnectorResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(ShutdownMode::Drain);
        }
        let _parent = self.observe_shutdown().await;
        self.children.join_all().await;
        let files = self.resources.close();
        self.confirm_retirement(files)
    }

    /// Actual parent/child join consumption and separate native inventory retirement.
    /// Retained original faults do not turn successful native cleanup into Unknown.
    pub fn retirement_complete(&self) -> bool {
        self.task_exit.is_some()
            && self.children.retirement_complete()
            && self.resources.retirement_complete()
    }

    /// Observe this same original JoinHandle without moving it out of the
    /// persistent application owner. Interrupted waits retain both handle and
    /// already collected original failure.
    pub async fn observe_shutdown(
        &mut self,
    ) -> Result<Option<EventDispatcherContinuation>, Arc<tokio::task::JoinError>> {
        if self.task_exit.is_none() {
            self.task_exit = Some((&mut self.task).await.map_err(Arc::new));
        }
        self.task_exit
            .as_ref()
            .expect("original task exit retained")
            .clone()
    }

    /// Diagnostic only; retirement still consumes the actual join and native providers.
    pub fn is_terminated(&self) -> bool {
        self.task_exit.is_some() || self.task.is_finished()
    }

    pub fn original_shutdown_failure(&self) -> Option<Arc<tokio::task::JoinError>> {
        self.task_exit
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .cloned()
    }

    pub fn original_child_failure_count(&self) -> usize {
        self.children.failure_count()
    }

    pub fn activate(&mut self) -> ConnectorResult<()> {
        if !matches!(self.init_result, Some(Ok(true))) {
            return Err(ConnectorError::Config(
                "dispatcher activation requires its actual Ready acknowledgement".into(),
            ));
        }
        if let Some(activate) = self.activate.take() {
            activate.send(()).map_err(|_| {
                ConnectorError::Config("dispatcher stopped before publication".into())
            })?;
        }
        Ok(())
    }

    fn ready_result(&self) -> ConnectorResult<bool> {
        match self
            .init_result
            .as_ref()
            .expect("readiness outcome retained")
        {
            Ok(ready) => Ok(*ready),
            Err(error) => Err(ConnectorError::RetainedPreparation(error.clone())),
        }
    }
    pub(super) fn wait_ready_sync(&mut self) -> ConnectorResult<bool> {
        if self.init_result.is_none() {
            self.init_result = Some(
                self.init_rx
                    .as_ref()
                    .expect("original readiness receiver")
                    .recv()
                    .unwrap_or(Err(ConnectorError::DispatcherStart))
                    .map_err(Arc::new),
            );
            self.init_rx = None;
        }
        match self.init_result.take().expect("readiness outcome retained") {
            Err(error) => {
                self.init_result = Some(Err(error.clone()));
                Err(ConnectorError::RetainedPreparation(error))
            }
            Ok(ready) => {
                self.init_result = Some(Ok(ready));
                Ok(ready)
            }
        }
    }
    pub async fn wait_ready(&mut self) -> ConnectorResult<bool> {
        while self.init_result.is_none() {
            let changed = self.init_notify.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self
                .init_rx
                .as_ref()
                .expect("original readiness receiver")
                .try_recv()
            {
                Ok(result) => {
                    self.init_result = Some(result.map_err(Arc::new));
                    self.init_rx = None;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.init_result = Some(Err(Arc::new(ConnectorError::DispatcherStart)));
                    self.init_rx = None;
                }
                Err(mpsc::TryRecvError::Empty) => changed.await,
            }
        }
        self.ready_result()
    }
    pub fn original_preparation_failure(&self) -> Option<Arc<ConnectorError>> {
        self.init_result
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .cloned()
    }

    /// Snapshot the current delivery status for all configured sinks.
    pub fn sink_status(&self) -> Vec<SinkStatus> {
        self.status_handle().sink_status()
    }

    pub fn status_handle(&self) -> EventDispatcherStatusHandle {
        EventDispatcherStatusHandle {
            sink_stats: self.sink_stats.clone(),
        }
    }
}

impl EventDispatcherStatusHandle {
    /// Snapshot the current delivery status for all configured sinks.
    pub fn sink_status(&self) -> Vec<SinkStatus> {
        self.sink_stats
            .lock()
            .expect("sink stats")
            .iter()
            .map(|(tag, stats)| stats.snapshot(tag.clone()))
            .collect()
    }
}
