//! Same original reconnect body and 1s..15s backoff; no join owner here.
use super::*;
impl State {
    pub(super) async fn rebuild(
        &self,
        failed_path: Arc<dyn PacketPathCarrier>,
        execution: PacketPathExecutionSource,
    ) -> Result<(), ChannelError> {
        let mut closing = self.closing.subscribe();
        let mut backoff = Duration::from_secs(1);
        loop {
            if *closing.borrow() || !Arc::ptr_eq(&failed_path, &self.path.borrow()) {
                return Ok(());
            }
            // A failed old Path can be referenced by watch/RX/TX callers.
            // Return its same unique views and settle its actual callee first;
            // retaining Arc<Path> itself is never a native-open permission.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let retired = tokio::select! {
                result=failed_path.retire_driver_handoff(&self.wire,deadline)=>Some(result),
                _=closing.changed()=>None,
            };
            let Some(retired) = retired else {
                return Ok(());
            };
            let can_build = match retired {
                Ok(report) => report.is_complete(),
                Err(source) => {
                    let source = Arc::new(source);
                    // Strong delivery is attempted on the exact source/slot
                    // ResourceAccess. A full receiver leaves the issuer's
                    // original receipt and controller pending; logging is not
                    // acceptance and never permits this worker to build new.
                    let received = execution.receive_cleanup_failure(source.clone())?;
                    tracing::warn!(
                        ?source,
                        ?received,
                        "same outer path original retirement retained"
                    );
                    false
                }
            };
            if can_build {
                let built = tokio::select! {
                    result=execution.build_carrier(&self.operation)=>Some(result),
                    _=closing.changed()=>None,
                };
                match built {
                    None => return Ok(()),
                    Some(Ok(next)) => {
                        *self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            Some(next.clone());
                        // Strong custody is installed before confirmation. No
                        // watch receiver can use the candidate before this gate.
                        next.confirm_driver_handoff(&self.wire)?;
                        self.path.send_replace(next);
                        let installed = self
                            .pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        drop(installed);
                        return Ok(());
                    }
                    Some(Err(source)) => {
                        tracing::warn!(?source, "raw-IP outer packet path reconnect failed")
                    }
                }
            }
            tokio::select! {
                _=tokio::time::sleep(backoff)=>{},
                _=closing.changed()=>return Ok(()),
            }
            backoff = (backoff * 2).min(Duration::from_secs(15));
        }
    }
    pub(super) async fn wait_background(&self) -> Result<(), EngineError> {
        let result = match self.actor() {
            Actor::Vacant => Ok(()),
            Actor::Registering => Err(failed(broken(
                "same outer reconnect registration is unresolved",
            ))),
            Actor::Registered(witness) => {
                // Only the real Scope consumes the one JoinHandle and Host
                // ACK. This waiter cannot synthesize either or refund its task.
                let joined = witness.wait_registration_released().await;
                joined
                    .result()
                    .map_err(|source| failed(ChannelError::retained(source)))
            }
        };
        result?;
        match self.request_failure.get() {
            Some(source) => Err(failed(ChannelError::retained(source.clone()))),
            None => Ok(()),
        }
    }
}
