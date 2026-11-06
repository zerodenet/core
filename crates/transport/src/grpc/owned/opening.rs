//! Selected native/prefix preparation on the original physical record.
use super::{state::Physical, *};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, PendingCarrierObserver},
        scope::TaskJoinWitness,
    },
    controlled_carrier::{CarrierDriver, CarrierGrant},
};
impl Managed {
    pub(super) async fn prepare_original(
        self: Arc<Self>,
        physical: Arc<Physical>,
        demand: Demand,
        grant: CarrierGrant,
        waiting: Option<PendingCarrierObserver>,
        caller: Option<TaskJoinWitness>,
    ) -> Result<CarrierDriver<crate::grpc::prepared::PreparedConnection>, ChannelError> {
        let call = CarrierContext {
            session: None,
            deadline: demand.deadline,
            cancel: physical.stop.clone(),
        };
        let pending = if self.options.selected_background {
            Some(super::pending::transport(
                self.clone(),
                physical.clone(),
                demand.binding.clone(),
                call.clone(),
                waiting,
                caller.ok_or_else(|| error(self.instance, Stage::Prepare, Reason::StalePlan))?,
            )?)
        } else {
            None
        };
        let selected = if let Some(pending) = &pending {
            self.wait(
                &demand,
                self.upstream.select_background_carrier_observed(
                    Some(demand.binding.clone()),
                    call.clone(),
                    pending.pending_carrier_observer(),
                ),
            )
            .await?
        } else {
            self.upstream.clone()
        };
        if selected.binding() != &demand.binding {
            return Err(error(self.instance, Stage::Prepare, Reason::StalePlan));
        }
        let io = if demand.binding.prefix.is_empty() {
            self.wait(
                &demand,
                selected.acquire_controlled_stream(
                    self.options.endpoint.clone(),
                    call.clone(),
                    grant,
                ),
            )
            .await?
        } else {
            let loan = self
                .wait(
                    &demand,
                    selected.borrow_stream(self.options.endpoint.clone(), call.clone()),
                )
                .await?;
            grant.adopt_stream_loan(
                selected.binding(),
                self.options.endpoint.clone(),
                &call,
                loan,
            )?
        };
        let preparing = io.prepare_driver(|io| async {
            let io = zero_platform_tokio::TcpRelayStream::new(super::carrier::Exclusive::new(io));
            #[cfg(feature = "tls")]
            let io = match &self.options.settings.tls {
                Some(tls) => {
                    crate::tls::connect_tls_stream(io, tls, None, &self.options.settings.server)
                        .await?
                }
                None => io,
            };
            let settings = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
                .map(|settings| settings.peer.clone());
            crate::grpc::prepared::prepare_connection(
                io,
                self.options.settings.grpc.clone(),
                settings.as_deref(),
            )
            .await
        });
        let prepared = self
            .wait(&demand, async {
                preparing.await.map_err(|source| {
                    error(self.instance, Stage::Handshake, Reason::Io).with_source(source)
                })
            })
            .await?;
        // Only the actual receiver's installed controller (or an actual cached
        // donor driver) transfers a pending foreign claim out of this record.
        physical.release_installed_pending();
        Ok(prepared)
    }
}
