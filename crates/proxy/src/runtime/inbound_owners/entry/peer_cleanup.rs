//! The original inline observer drives joined children as well as Scope exits.
use super::*;
use futures_util::future::BoxFuture;

impl InboundOwner {
    pub(super) async fn observe_peer_driver(
        &self,
        scope: &mut TaskScope,
    ) -> Result<(), EngineError> {
        let mut cleanup: Option<BoxFuture<'_, Result<(), ChannelError>>> = None;
        // A previous observer may have been cancelled after consuming TaskExit
        // while its original native/mapping owner was still pending.
        let mut dirty = true;
        while !self.driver_finished.load(Ordering::Acquire) {
            if dirty && cleanup.is_none() {
                dirty = false;
                cleanup = Some(Box::pin(self.retire_joined_peers()));
            }
            tokio::select! {
                exit = scope.next_exit() => {
                    let exit = exit.ok_or_else(|| std::io::Error::other("listener driver exit remains unobserved"))?;
                    let failed = self.record(exit); // persist original ID/source before awaiting cleanup
                    dirty = true;
                    if failed { self.stop(); }
                }
                result = async { cleanup.as_mut().expect("enabled cleanup").as_mut().await }, if cleanup.is_some() => {
                    cleanup = None;
                    if let Err(mut error) = result {
                        error.component.get_or_insert(self.identity.component);
                        self.failure.lock().expect("inbound failure poisoned").get_or_insert_with(|| Arc::new(error).into());
                        // No retained owner or unknown native outcome is released.
                        self.stop();
                    }
                }
            }
        }
        self.result()
    }
    async fn retire_joined_peers(&self) -> Result<(), ChannelError> {
        let peers = self.peers.lock().expect("peer owners poisoned").clone();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let mut first_failure = None;
        let receivers = self.accepted_receiver.lock().unwrap().clone();
        for accepted in receivers {
            if let Err(error) = accepted.retire_logical_resources(deadline).await {
                let source = Arc::new(error);
                self.failure
                    .lock()
                    .expect("inbound failure poisoned")
                    .get_or_insert(source.clone().into());
                self.stop();
                first_failure.get_or_insert_with(|| retained(source));
            }
        }
        for group in peers {
            // This future stays pinned while the same observer consumes other
            // peers' real exits. Pending close cannot block those Scope joins.
            if let Err(mut error) = async {
                group.retire_logical_resources(deadline).await?;
                group.retire_joined(deadline).await
            }
            .await
            {
                error.component.get_or_insert(self.identity.component);
                let source = group.first_fault().unwrap_or_else(|| Arc::new(error));
                self.failure
                    .lock()
                    .expect("inbound failure poisoned")
                    .get_or_insert(source.clone().into());
                // Persist the original source and revoke admission before any
                // other group's cleanup can suspend or call back into the host.
                self.stop();
                first_failure.get_or_insert_with(|| retained(source));
            }
        }
        // Wake only after the real joined-child/native cleanup pass. This
        // signal grants no quiescence/closure: the source must read its exact
        // closing futures and actual active/preparing/child/claiming tables.
        self.acquisition.peer_cleanup_progressed();
        first_failure.map_or(Ok(()), Err)
    }
}
