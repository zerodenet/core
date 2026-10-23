use tokio::time::Instant as TokioInstant;
use zero_core::Session;
use zero_engine::EngineError;

use super::super::super::super::contract::{
    UpstreamAssociationCloseReason, UpstreamAssociationTarget, UpstreamAssociationTransport,
};
use super::super::model::UpstreamAssociationRuntime;
use crate::protocol_registry::UdpRuntimeServices;

impl<T, A> UpstreamAssociationRuntime<T, A>
where
    T: UpstreamAssociationTarget,
    A: UpstreamAssociationTransport<T>,
{
    pub(crate) async fn send_packet(
        &mut self,
        services: &UdpRuntimeServices,
        inbound_tag: &str,
        association: T,
        session: &Session,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        self.ensure_association(services, inbound_tag, association, session)
            .await?;

        let association_ref = self
            .upstream
            .association()
            .expect("successful establish stores upstream association");

        match association_ref
            .send_packet(&session.target, session.port, payload)
            .await
        {
            Ok(sent) => {
                services.record_udp_upstream_packet_sent();
                self.idle_deadline =
                    Some(TokioInstant::now() + services.udp_upstream_idle_timeout());
                Ok(sent)
            }
            Err(error) => {
                services.record_udp_upstream_send_failure();
                self.drop_after_send_error(inbound_tag, &error);
                Err(error)
            }
        }
    }

    async fn ensure_association(
        &mut self,
        services: &UdpRuntimeServices,
        inbound_tag: &str,
        association: T,
        session: &Session,
    ) -> Result<(), EngineError> {
        let needs_new_association = !self.upstream.matches_target(&association);

        if !needs_new_association {
            services.record_udp_upstream_association_reused();
            let (outbound_tag, server, port) = association.log_parts();
            crate::logging::log_udp_upstream_association_reused(
                inbound_tag,
                outbound_tag,
                server,
                port,
            );
            return Ok(());
        }

        if let Some(a) = self.upstream.take() {
            let (_, association) = a.into_parts();
            association.close(UpstreamAssociationCloseReason::Closed);
            self.idle_deadline = None;
        }

        // Business facts and ingress identity are derived by the runtime before
        // crossing into an adapter. The transport never receives the registry.
        let established = async {
            let execution = services.datagram_execution(inbound_tag, session)?;
            A::establish(
                execution,
                services.association_observer(),
                association.clone(),
            )
            .await
        }
        .await;
        match established {
            Ok(a) => {
                services.record_udp_upstream_association_created();
                self.idle_deadline =
                    Some(TokioInstant::now() + services.udp_upstream_idle_timeout());
                let (outbound_tag, server, port) = association.log_parts();
                crate::logging::log_udp_upstream_association_created(
                    inbound_tag,
                    outbound_tag,
                    server,
                    port,
                    services.udp_upstream_idle_timeout(),
                );
                let _ = self.upstream.insert(association, a);
                Ok(())
            }
            Err(error) => {
                services.record_udp_upstream_association_failed();
                Err(error)
            }
        }
    }
}
