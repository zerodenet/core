//! The actual Direct lookup stays in its existing caller/auxiliary Scope.
//! A raw-IP Source retains only its exact query receipts, not the DNS pool.
use super::*;
use std::sync::atomic::Ordering;

impl ControlledCandidates {
    pub(super) async fn prepare_resolution<'a, T: Send + 'a>(
        &self,
        call: zero_dns::DnsResolutionCall,
        work: BoxFuture<'a, T>,
    ) -> Result<BoxFuture<'a, std::io::Result<T>>, ChannelError> {
        if matches!(self.demand.request, DirectDemand::Business(_))
            && !self.dial_started.load(Ordering::Acquire)
        {
            self.resolved_domain.store(true, Ordering::Release);
            self.generation
                .store(self.services.network_generation(), Ordering::Release);
        }
        #[cfg(feature = "raw-ip-runtime")]
        if let Some(caller) = self.demand.request.native_caller() {
            // Prepare return guards before admission; even unpolled work must
            // return this exact call if admission or its await is cancelled.
            let work = call.scope(work);
            let deadline = Instant::now()
                .checked_add(self.demand.retirement)
                .ok_or_else(|| {
                    invalid(
                        Reason::InvalidConfig,
                        "Direct DNS retirement deadline overflow",
                    )
                })?;
            let permit = caller.bind_dns_resolution(call, deadline).await?;
            return Ok(Box::pin(async move {
                let _permit = permit;
                work.await
            }));
        }
        // Other callers already have their canonical DNS ownership. No new
        // Source quota, query or background task is introduced for them.
        drop(call);
        Ok(Box::pin(async move { Ok(work.await) }))
    }
}
