//! Same original cleanup, independently of caller/Scope/archive completion.
use super::*;
use zero_platform_tokio::contract::lifecycle::{CloseReport, ResourceLifecycle};
impl Runtime {
    pub fn revoke(&self) {
        match self {
            Self::Quic { generation, .. } => {
                generation.cancel.cancel();
                if let Some((pool, _)) = &generation.options.h3 {
                    pool.retire_owned(generation.instance, &generation.tasks.scope_identity());
                }
            }
            Self::Tcp { generation, .. } => {
                generation.cancel.cancel();
                generation
                    .options
                    .pool
                    .retire_owned(generation.instance, &generation.tasks.scope_identity());
            }
        }
        // Root still owns the unique IO/queues/controller. Never cancel the
        // cleanup token as a substitute for its actual accepted-frame drain.
    }
    pub async fn shutdown_originals(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<CloseReport, ChannelError> {
        let mut report = CloseReport::default();
        match self {
            Self::Quic {
                generation,
                carrier,
                failures,
                ..
            } => {
                let (metadata, stream) = failures.branches();
                tokio::join!(
                    async {
                        metadata.capture(
                            generation
                                .metadata
                                .shutdown_after_registration_ack(deadline)
                                .await,
                        )
                    },
                    async { stream.capture(carrier.shutdown(deadline).await) },
                );
                failures.report_to(
                    generation.resources.as_ref(),
                    &generation.tasks.scope_identity(),
                    &mut report,
                )?;
                let native = generation.metadata.registration();
                if !native
                    .closed()
                    .is_some_and(|closed| closed.matches(&native))
                    || !carrier_returned(carrier)
                {
                    report.unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "actual XHTTP metadata/carrier original receipts remain pending".into(),
                        ),
                    );
                }
                if report.is_complete() {
                    generation.cleanup.cancel();
                }
            }
            Self::Tcp {
                generation,
                carrier,
                failures,
                ..
            } => {
                let (_, stream) = failures.branches();
                stream.capture(carrier.shutdown(deadline).await);
                failures.report_to(
                    generation.resources.as_ref(),
                    &generation.tasks.scope_identity(),
                    &mut report,
                )?;
                if !carrier_returned(carrier) {
                    report.unfinished.push(
                        zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                            "actual XHTTP TCP native/users original receipts remain pending".into(),
                        ),
                    );
                }
                if report.is_complete() {
                    generation.cleanup.cancel();
                }
            }
        }
        Ok(report)
    }
}

fn carrier_returned(owner: &CarrierOwner) -> bool {
    let registration = owner.registration();
    registration
        .closed()
        .is_some_and(|closed| closed.matches(&registration))
        && registration
            .users_closed()
            .is_some_and(|closed| closed.matches(&registration))
}
