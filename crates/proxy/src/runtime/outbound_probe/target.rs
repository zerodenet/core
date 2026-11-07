//! One connect-only server probe on the actual host's existing diagnostic owner.
use super::*;
use zero_platform_tokio::contract::CancellationToken;
impl OutboundProbeRuntime {
    pub(crate) async fn probe_server_endpoint(
        &self,
        host: String,
        port: u16,
        stop: Option<CancellationToken>,
    ) -> Result<(), OutboundProbeError> {
        let reserve = zero_runtime::reserve_probe();
        tokio::pin!(reserve);
        let credit=match &stop{
        Some(stop)=>tokio::select!{biased;_=stop.cancelled()=>return Err(cancelled_probe()),credit=&mut reserve=>credit},
        None=>reserve.await,
    }.map_err(|source|OutboundProbeError::new("probe_unavailable",source.to_string()))?;
        let end = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        // The original request also retains this same reservation until the
        // actual Node query dependencies have returned Native/Scope/Host ACK.
        let _request_credit = credit.clone();
        let resolution = self.services.resolver().resolution_call();
        let original = Arc::new(ProbeOriginal::default());
        let pending = original.clone();
        let installed = original.clone();
        let services = self.services.clone();
        let observed: zero_platform_tokio::contract::BoxFuture<
            'static,
            std::io::Result<Result<(), zero_platform_tokio::contract::ChannelError>>,
        > = Box::pin(resolution.scope(async move {
            let call = installed
                .call
                .lock()
                .expect("probe original poisoned")
                .clone()
                .expect("actual ProbeOwner installs before polling work");
            drop(installed);
            crate::component::direct_bridge::connect_target(&services, host, port, call).await
        }));
        let run = self.services.protocols().runtime_host();
        let config = self.services.snapshot().config().clone();
        let work = run.run_probe(
            &config,
            None,
            credit,
            zero_core::request::ServicePurpose::DiagnosticProbe,
            end,
            move |call| {
                pending.install(call);
                drop(pending);
                observed
            },
        );
        tokio::pin!(work);
        let result = match stop {
            Some(stop) => {
                tokio::select! {biased;_=stop.cancelled()=>{original.cancel();work.await},result=&mut work=>result}
            }
            None => work.await,
        };
        // Deliver any actual Probe ownership source before independently
        // progressing its original Node query dependencies.
        let result = result
            .map_err(OutboundProbeError::from_probe_failure)
            .and_then(|result| {
                result.map_err(|source| OutboundProbeError::from_engine(source.into()))
            })
            .and_then(|result| {
                result.map_err(|source| {
                    OutboundProbeError::from_engine(std::io::Error::other(source).into())
                })
            });
        let cleanup = resolution
            .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
            .await;
        match cleanup {
            Ok(closed) if closed.matches(&resolution) => result,
            cleanup => {
                let source = Arc::new(match cleanup {
                    Err(source) => source,
                    Ok(_) => std::io::Error::other(
                        "fixed target DNS returned another resolution's original",
                    ),
                });
                let _ = zero_runtime::ApplyObservation::retain_cleanup_failure(
                    zero_runtime::CommandCleanupKind::Resolution,
                    source.clone(),
                );
                match result {
                    Err(work) => Err(work.with_cleanup(source)),
                    Ok(()) => Err(OutboundProbeError::from_engine(
                        std::io::Error::new(source.kind(), source).into(),
                    )),
                }
            }
        }
    }
}
#[cfg(all(test, feature = "managed-stream-runtime"))]
mod tests;
