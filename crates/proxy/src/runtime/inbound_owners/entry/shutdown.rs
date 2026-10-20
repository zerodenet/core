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
            scope.begin_shutdown();
            loop {
                match timeout_at(deadline, scope.next_exit()).await {
                    Ok(Some(exit)) => self.record(exit),
                    Ok(None) => return Ok::<(), std::io::Error>(()),
                    Err(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "inbound tasks remain unconfirmed",
                        ))
                    }
                }
                // Dispatch each actual receipt before the next await. Dynamic
                // peer drivers may be waiting for precisely this joined exit;
                // collecting an all-tasks report first would deadlock them.
            }
        };
        let close_peers = async {
            #[cfg(feature = "managed-datagram-runtime")]
            {
                let groups = self.peers.lock().expect("peer owners poisoned").clone();
                for group in groups {
                    group.wait_closed().await.map_err(std::io::Error::other)?;
                }
            }
            Ok::<(), std::io::Error>(())
        };
        let result = timeout_at(deadline, async {
            tokio::try_join!(close_resources, close_tasks, close_peers).map(|_| ())
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
