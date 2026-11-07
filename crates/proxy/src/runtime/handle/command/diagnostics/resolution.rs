//! Observe the original Default DNS algorithm and await both actual families.
use std::{future::Future, io, sync::Arc};
pub(super) async fn settled<T>(
    resolver: &zero_dns::DnsSystem,
    work: impl Future<Output = T>,
) -> io::Result<T> {
    let call = resolver.resolution_call();
    let stop = zero_runtime::ApplyObservation::caller_stop();
    let original = call.scope(work);
    tokio::pin!(original);
    let work = match stop {
        Some(stop) => {
            tokio::select! {biased;_=stop.cancelled()=>{call.cancel();original.await},work=&mut original=>work}
        }
        None => original.await,
    };
    // Caller completion is never the provider's native/Query/Scope/Host ACK proof.
    let cleanup = call
        .wait_closed(tokio::time::Instant::now() + std::time::Duration::from_secs(5))
        .await;
    let cleanup = match cleanup {
        Ok(closed) if closed.matches(&call) => {
            let mut faults = Vec::new();
            for query in closed.queries() {
                if let Some(source) = query.failure() {
                    faults.push(Arc::new(io::Error::other(source.clone())));
                }
                if let Some(source) = query.cleanup_failure() {
                    faults.push(source.clone());
                }
            }
            if faults.is_empty() {
                Ok(())
            } else {
                Err(io::Error::other(ResolutionFaults(faults)))
            }
        }
        Ok(_) => Err(io::Error::other(
            "DNS resolution returned another caller's retirement",
        )),
        Err(source) => Err(source),
    };
    if let Err(source) = cleanup {
        let source = Arc::new(source);
        // Actual Query faults also remain in their canonical QueryFaults/Slot owner.
        // The same request record separately receives the exact cleanup source;
        // missing CURRENT never fabricates a receiver or a successful close receipt.
        let _ = zero_runtime::ApplyObservation::retain_cleanup_failure(
            zero_runtime::CommandCleanupKind::Resolution,
            source.clone(),
        );
        return Err(io::Error::new(source.kind(), source));
    }
    work
}
#[derive(Debug)]
struct ResolutionFaults(Vec<Arc<io::Error>>);
impl std::fmt::Display for ResolutionFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for source in &self.0 {
            write!(f, "; {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for ResolutionFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|e| e.as_ref() as _)
    }
}

#[cfg(all(test, feature = "dns"))]
mod tests;
