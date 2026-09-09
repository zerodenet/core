use crate::runtime::udp_socket::DirectUdpResponseSource;

use zero_core::{InboundUdpAssociation, InboundUdpAssociationResponder};
use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;

use super::dispatch::{UdpAssociationDispatchBridge, UdpAssociationDispatchOutcome};
use super::model::UdpAssociationDatagramRequest;
use super::response::{write_peer_response, write_target_response};
use crate::runtime::udp_delivery::write_direct_response as write_direct_udp_response;
use crate::runtime::udp_delivery::{
    record_direct_udp_response_parts, UdpChainResponseParts, UdpUpstreamResponseParts,
};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_ingress::UdpIngressRuntime;

pub(crate) trait UdpAssociationHandler {
    async fn handle_client_datagram(
        &mut self,
        request: UdpAssociationDatagramRequest<'_>,
    ) -> Result<(), EngineError>;

    async fn write_direct_response(
        &mut self,
        runtime: &UdpIngressRuntime,
        dispatch: &UdpDispatch,
        relay: &TokioDatagramSocket,
        sender: DirectUdpResponseSource,
        payload: &[u8],
    ) -> Result<(), EngineError>;

    async fn write_upstream_response(
        &mut self,
        relay: &TokioDatagramSocket,
        response: &UdpUpstreamResponseParts,
    ) -> Result<usize, EngineError>;

    async fn write_chain_response(
        &mut self,
        relay: &TokioDatagramSocket,
        response: &UdpChainResponseParts,
    ) -> Result<usize, EngineError>;
}

impl<H> UdpAssociationHandler for H
where
    H: InboundUdpAssociation + InboundUdpAssociationResponder,
{
    async fn handle_client_datagram(
        &mut self,
        request: UdpAssociationDatagramRequest<'_>,
    ) -> Result<(), EngineError> {
        let UdpAssociationDatagramRequest {
            runtime,
            dispatch,
            relay,
            pending_control_traffic,
            sender,
            payload,
        } = request;
        let inbound_tag = dispatch.inbound_tag().to_owned();
        let source_addr = sender;
        let sender = zero_platform_tokio::socket_addr_to_socket_address(sender);
        let mut dispatch_bridge = UdpAssociationDispatchBridge::new(
            runtime,
            dispatch,
            pending_control_traffic,
            source_addr,
        );
        let result = match self
            .dispatch_datagram(sender, payload, &mut dispatch_bridge)
            .await
        {
            Err(error) => Err(error),
            Ok(()) => match dispatch_bridge.take_outcome() {
                UdpAssociationDispatchOutcome::ClientHandled => Ok(()),
                UdpAssociationDispatchOutcome::PeerResponse { sender, payload } => {
                    let sender_socket_addr =
                        zero_platform_tokio::socket_address_to_socket_addr(sender);
                    let response = record_direct_udp_response_parts(
                        runtime.services(),
                        dispatch_bridge.dispatch(),
                        DirectUdpResponseSource {
                            sender: sender_socket_addr,
                            session_id: None,
                        },
                        &payload,
                    );
                    write_direct_udp_response(&response, || async {
                        write_peer_response(self, relay, sender, &payload).await
                    })
                    .await?;
                    Ok(())
                }
                UdpAssociationDispatchOutcome::UnexpectedSender { sender } => {
                    tracing::debug!(?sender, "dropping udp packet from unexpected sender");
                    Ok(())
                }
            },
        };

        if let Err(error) = result {
            tracing::warn!(
                inbound_tag,
                protocol = "udp_association",
                error = %error,
                "failed to process UDP association packet"
            );
        }

        Ok(())
    }

    async fn write_direct_response(
        &mut self,
        runtime: &UdpIngressRuntime,
        dispatch: &UdpDispatch,
        relay: &TokioDatagramSocket,
        sender: DirectUdpResponseSource,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let response =
            record_direct_udp_response_parts(runtime.services(), dispatch, sender, payload);
        write_direct_udp_response(&response, || async {
            write_target_response(
                self,
                relay,
                &response.target,
                response.port,
                response.payload,
            )
            .await
        })
        .await?;

        Ok(())
    }

    async fn write_upstream_response(
        &mut self,
        relay: &TokioDatagramSocket,
        response: &UdpUpstreamResponseParts,
    ) -> Result<usize, EngineError> {
        write_target_response(
            self,
            relay,
            &response.target,
            response.port,
            &response.payload,
        )
        .await
    }

    async fn write_chain_response(
        &mut self,
        relay: &TokioDatagramSocket,
        response: &UdpChainResponseParts,
    ) -> Result<usize, EngineError> {
        write_target_response(
            self,
            relay,
            &response.target,
            response.port,
            &response.payload,
        )
        .await
    }
}
