use std::any::Any;

use super::super::super::super::flow::ManagedUdpFlowResume;
use super::super::super::connector::ManagedDatagramFlowConnector;
use super::super::mismatch::managed_mismatch;
use super::super::model::ManagedDatagramFlowManager;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::path::OutboundEndpoint;
use crate::runtime::udp_flow::managed::model::ManagedDatagramExistingSend;
use crate::runtime::udp_flow::packet_path::{UdpFlowContext, UdpPacketRef};
use crate::runtime::udp_flow::result::FlowFailure;

impl<T, C> ManagedDatagramFlowManager<T, C>
where
    T: Any + Clone + Send + Sync + std::fmt::Debug + 'static,
    C: ManagedDatagramFlowConnector<T>,
{
    pub(super) fn supports_managed_existing(&self, resume: &ManagedUdpFlowResume) -> bool {
        resume.as_ref::<T>().is_some()
    }

    async fn send(
        &mut self,
        ctx: UdpFlowContext<'_>,
        services: Option<UdpRuntimeServices>,
        endpoint: OutboundEndpoint,
        resume: T,
        packet_ref: UdpPacketRef<'_>,
    ) -> Result<usize, FlowFailure> {
        let upstream = endpoint.upstream();
        let cache_key = self
            .connector
            .connector_flow(&resume, endpoint.clone(), ctx.session_id)
            .into_cache_key();
        let establish = self
            .connector
            .establish(services, endpoint, resume, packet_ref);
        let result = if C::INITIAL_PACKET_PRE_SENT {
            self.upstreams
                .send_or_insert_pre_sent_key(
                    cache_key,
                    ctx.chain_tasks,
                    ctx.session_id,
                    packet_ref,
                    establish,
                )
                .await
        } else {
            self.upstreams
                .send_or_insert_key(
                    cache_key,
                    ctx.chain_tasks,
                    ctx.session_id,
                    packet_ref,
                    establish,
                )
                .await
        };
        result.map_err(|error| FlowFailure {
            stage: self.establish_stage,
            error,
            upstream: Some(upstream),
        })
    }

    pub(super) async fn send_managed_existing(
        &mut self,
        request: ManagedDatagramExistingSend<'_>,
    ) -> Result<usize, FlowFailure> {
        let Some(resume) = request.resume.cloned::<T>() else {
            return Err(managed_mismatch(
                self.mismatch_stage,
                request.server,
                request.port,
                self.mismatch_message,
            ));
        };
        self.send(
            UdpFlowContext {
                chain_tasks: request.chain_tasks,
                session_id: request.session_id,
            },
            request.services,
            OutboundEndpoint {
                server: request.server.to_owned(),
                port: request.port,
            },
            resume,
            UdpPacketRef {
                target: request.target,
                port: request.target_port,
                payload: request.payload,
            },
        )
        .await
    }
}
