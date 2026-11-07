//! All public diagnostic entry points share the actual finite Service owner.
use super::*;
impl Proxy {
    pub(super) fn with_diagnostic_service<T: Send + 'static>(
        &self,
        work: impl Future<Output = T> + Send + 'static,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<T, EngineError>> {
        // This one original command's work remains in the same task/Scope.
        // Erase its large concrete state before warm/cold maintenance, rather
        // than copying it through each nested generic admission/Apply future.
        let work: zero_platform_tokio::contract::BoxFuture<'static, T> = Box::pin(work);
        Box::pin(async move {
            let snapshot = self.engine().runtime_snapshot();
            if self
                .protocols
                .current_components(snapshot.config())
                .is_some()
            {
                return self
                    .protocols
                    .drive_component_supervision(async {
                        tokio::select! { biased;
                            failure = self.protocols.component_failure() => Err(failure),
                            result = work => Ok(result),
                        }
                    })
                    .await;
            }
            let proxy = self.clone();
            let cancel = zero_platform_tokio::contract::CancellationToken::new();
            struct CancelOnDrop(zero_platform_tokio::contract::CancellationToken);
            impl Drop for CancelOnDrop {
                fn drop(&mut self) {
                    self.0.cancel();
                }
            }
            let _waiter = CancelOnDrop(cancel.clone());
            // The result is nested so a native lifecycle error retains its source;
            // it is not flattened into the task owner's acknowledgement string.
            self.host
            .apply()
            .run(self.host.apply_lock().clone(), async move {
                let config = proxy.engine().runtime_snapshot().config().clone();
                let result = proxy
                    .protocols
                    .maintain_service_components(
                        config,
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        || async {
                            let result=tokio::select!{biased;_=cancel.cancelled()=>Err(EngineError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted,"diagnostic caller cancelled"))),result=work=>Ok(result)};
                            proxy.host.cancel_probes();
                            proxy.host.drain_probes(tokio::time::Instant::now()+std::time::Duration::from_secs(5)).await.map_err(|source|EngineError::Io(std::io::Error::other(source)))?;
                            result
                        },
                    )
                    .await
                    .map_err(|source| EngineError::Io(std::io::Error::other(source))).and_then(|result|result);
                Ok(result)
            })
            .await
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?
        })
    }
}
