use super::*;
use tokio::time::timeout_at;
impl InboundOwner {
    pub(crate) async fn shutdown(&self, deadline: Instant) -> Result<(), EngineError> {
        self.stop();
        let resources = self
            .resources
            .lock()
            .expect("inbound resources poisoned")
            .clone();
        let close_resources = async {
            for resource in resources.into_iter().rev() {
                if resource.stopped.load(Ordering::Acquire) {
                    continue;
                }
                let mut owner = resource.owner.lock().await;
                owner.revoke(CloseReason::Shutdown);
                let report = owner.shutdown(deadline).await.map_err(|mut error| {
                    error.component.get_or_insert(resource.identity.component);
                    let error = Arc::new(error);
                    self.failure
                        .lock()
                        .expect("inbound failure poisoned")
                        .get_or_insert(error.clone());
                    std::io::Error::other(retained(error))
                })?;
                let complete = report.is_complete();
                self.report
                    .lock()
                    .expect("inbound close report poisoned")
                    .append(report);
                if !complete {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "inbound carrier cleanup remains unconfirmed",
                    ));
                }
                resource.stopped.store(true, Ordering::Release);
            }
            Ok::<(), std::io::Error>(())
        };
        let close_tasks = async {
            let mut scope = self.scope.lock().await;
            let mut report = scope.shutdown(deadline).await;
            let complete = report.is_complete();
            for mut failure in report.failed_tasks.drain(..) {
                failure
                    .error
                    .component
                    .get_or_insert(self.identity.component);
                let error = Arc::new(failure.error);
                self.failure
                    .lock()
                    .expect("inbound failure poisoned")
                    .get_or_insert(error.clone());
                self.report
                    .lock()
                    .expect("inbound close report poisoned")
                    .failed_tasks
                    .push(TaskFailure {
                        id: failure.id,
                        class: failure.class,
                        error: retained(error),
                    });
            }
            if self
                .driver
                .get()
                .is_some_and(|id| report.exited_tasks.contains(id))
            {
                self.driver_finished.store(true, Ordering::Release);
            }
            // Receipt lives in the persistent entry before this branch returns;
            // cancellation while carrier shutdown is still pending cannot lose it.
            self.report
                .lock()
                .expect("inbound close report poisoned")
                .append(report);
            if complete {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "inbound tasks remain unconfirmed",
                ))
            }
        };
        let result = timeout_at(deadline, async {
            tokio::try_join!(close_resources, close_tasks).map(|_| ())
        })
        .await;
        match result {
            Ok(Ok(())) => self.result(),
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "inbound owner shutdown remains unconfirmed",
            )
            .into()),
        }
    }
}
