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
            let mut failure = None;
            for resource in resources.into_iter().rev() {
                if resource.stopped.load(Ordering::Acquire) {
                    continue;
                }
                let mut owner = resource.owner.lock().await;
                owner.revoke(CloseReason::Shutdown);
                let mut report = match owner.shutdown(deadline).await {
                    Ok(report) => report,
                    Err(mut error) => {
                        error.component.get_or_insert(resource.identity.component);
                        let error = Arc::new(error);
                        self.failure
                            .lock()
                            .expect("inbound failure poisoned")
                            .get_or_insert(error.clone());
                        failure.get_or_insert_with(|| std::io::Error::other(retained(error)));
                        continue;
                    }
                };
                self.retain_resource_failures(&mut report);
                let complete = report.is_complete();
                // A current unfinished resource is represented by its actual
                // retained owner/stopped flag. Do not grow deadline history
                // on each retry or keep an old timeout after confirmed close.
                report.unfinished.clear();
                self.report
                    .lock()
                    .expect("inbound close report poisoned")
                    .append(report);
                if complete {
                    resource.stopped.store(true, Ordering::Release);
                } else {
                    failure.get_or_insert_with(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "inbound carrier cleanup remains unconfirmed",
                        )
                    });
                }
            }
            failure.map_or(Ok::<(), std::io::Error>(()), Err)
        };
        let close_tasks = async {
            let mut scope = self.scope.lock().await;
            scope.begin_shutdown();
            loop {
                match timeout_at(deadline, scope.next_exit()).await {
                    Ok(Some(exit)) => {
                        self.record(exit);
                    }
                    Ok(None) => {
                        self.scope_closed.store(true, Ordering::Release);
                        return Ok::<(), std::io::Error>(());
                    }
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
                let mut failure = None;
                for group in groups {
                    if let Err(error) = group.wait_closed().await {
                        failure.get_or_insert_with(|| std::io::Error::other(error));
                    }
                    // A group's physical completion must not erase its
                    // actual sticky task source when no live observer remains.
                    if let Some(source) = group.first_fault() {
                        self.failure
                            .lock()
                            .expect("inbound failure poisoned")
                            .get_or_insert(source);
                    }
                }
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Ok::<(), std::io::Error>(())
        };
        let result = timeout_at(deadline, async {
            let (resources, tasks, peers, bound) = tokio::join!(
                close_resources,
                close_tasks,
                close_peers,
                self.close_pending_bound(deadline)
            );
            // A fault does not cancel a sibling's actual join/close operation.
            resources.and(tasks).and(peers).and(bound)
        })
        .await;
        if self.confirm_physical_close() {
            // Failure reporting and actual physical completion are independent.
            return self.result();
        }
        // A retained pre-execution native listener has no close receipt yet.
        // Empty Scope completion cannot clear its original Unknown source.
        self.pending_native_result()?;
        match result {
            Ok(Err(error)) => Err(error.into()),
            Ok(Ok(())) => {
                self.result()?;
                Err(std::io::Error::other("inbound physical confirmation is incomplete").into())
            }
            Err(_) => {
                self.result()?;
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "inbound owner shutdown remains unconfirmed",
                )
                .into())
            }
        }
    }
}
