use super::super::mismatch::managed_mismatch;
use super::super::model::{ManagedStreamFlowManager, ManagedStreamRelayRequest};
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::managed::model::ManagedRelayExistingSend;
use crate::runtime::udp_flow::managed::stream_manager::ManagedStreamFlowConnector;
use crate::runtime::udp_flow::packet_path::{UdpFlowContext, UdpPacketRef};
use crate::runtime::udp_flow::result::FlowFailure;

impl<T> ManagedStreamFlowManager<T>
where
    T: ManagedStreamFlowConnector,
{
    pub(super) async fn send_relay(
        &mut self,
        request: ManagedStreamRelayRequest<'_, T>,
    ) -> Result<usize, FlowFailure> {
        let ManagedStreamRelayRequest {
            ctx,
            carrier,
            tls_server_name,
            services,
            session,
            endpoint,
            resume,
            packet_ref,
        } = request;
        let session_id = ctx.session_id;
        let upstream = endpoint.upstream();
        let (cache_key, _) = resume
            .connector_flow(endpoint.clone(), session_id)
            .into_parts();
        let entry = resume
            .establish_relay(carrier, tls_server_name, services, session, endpoint)
            .await
            .map_err(|error| FlowFailure {
                stage: self.relay_establish_stage,
                error,
                upstream: Some(upstream.clone()),
            })?;

        self.upstreams
            .insert_and_send_key(cache_key, ctx.chain_tasks, session_id, packet_ref, entry)
            .await
            .map_err(|error| FlowFailure {
                stage: self.relay_send_stage,
                error,
                upstream: Some(upstream),
            })
    }

    pub(super) async fn send_managed_relay_existing(
        &mut self,
        request: ManagedRelayExistingSend<'_>,
    ) -> Result<usize, FlowFailure> {
        let Some(resume) = request.resume.cloned::<T>() else {
            return Err(managed_mismatch(
                self.mismatch_stage,
                request.server,
                request.port,
                self.mismatch_message,
            ));
        };
        self.send_relay(ManagedStreamRelayRequest {
            ctx: UdpFlowContext {
                chain_tasks: request.chain_tasks,
                session_id: request.session_id,
            },
            carrier: request.carrier,
            tls_server_name: request.tls_server_name,
            services: request.services,
            session: request.session,
            endpoint: OutboundEndpoint {
                server: request.server.to_owned(),
                port: request.port,
            },
            resume,
            packet_ref: UdpPacketRef {
                target: request.target,
                port: request.target_port,
                payload: request.payload,
            },
        })
        .await
    }
}
