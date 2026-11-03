//! Standard requests retain the actual wire codec and defer business identity to host.
use super::*;
use crate::component::inbound::{datagram, fallback};
use zero_core::{InboundFallbackCapture, ProtocolType};
use zero_platform_tokio::contract::{
    context::{HandshakeContext, InboundConnectionFacts},
    inbound::*,
    ChannelError,
};
mod mkcp;
mod quic;
mod streams;
impl VlessInboundListenerRequest {
    pub async fn accept_standard_request(
        self,
        mut socket: TokioSocket,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let facts = if self.accepts_proxy_protocol() {
            let facts = zero_transport::proxy_protocol::prepare(&mut socket)
                .await
                .map_err(datagram::failure)?;
            context
                .connection
                .as_ref()
                .ok_or_else(|| {
                    datagram::failure(std::io::Error::other(
                        "host omitted trusted prelude facts handoff",
                    ))
                })?
                .set(facts.clone())
                .map_err(|_| {
                    datagram::failure(std::io::Error::other(
                        "trusted prelude facts already published",
                    ))
                })?;
            facts
        } else {
            InboundConnectionFacts::from_stream(&socket)
        };
        if self.has_multiplexed_transport() {
            return self.accept_standard_transport(socket, facts, context).await;
        }
        let caller = context.accepted.as_ref().ok_or_else(|| {
            ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
                zero_platform_tokio::contract::Impact::Session,
            )
        })?;
        let mut facts = facts;
        let accepted = self
            .transport
            .clone()
            .accept_tcp_inbound(socket, caller)
            .await
            .map_err(datagram::failure)?;
        match accepted {
            plan::VlessTcpInboundAcceptResult::Control(control) => {
                Ok(AcceptedRequest::Control(control))
            }
            plan::VlessTcpInboundAcceptResult::FallbackReplay(mut replay) => {
                replay
                    .select_route(
                        &self.transport.fallback_policy,
                        None,
                        None,
                        facts.effective_source,
                        facts.local_endpoint,
                    )
                    .await
                    .map_err(datagram::failure)?;
                Ok(AcceptedRequest::Fallback(fallback::accepted(replay)?))
            }
            plan::VlessTcpInboundAcceptResult::Stream { stream, sni, alpn } => {
                facts.sni = sni;
                let stream = session::decrypt_stream(self.transport.decryption.as_ref(), stream)
                    .await
                    .map_err(datagram::failure)?;
                self.accept_standard_stream(record_client_stream(stream), facts, alpn, context)
                    .await
            }
        }
    }
    pub(super) async fn accept_standard_stream<S>(
        self,
        mut stream: S,
        facts: InboundConnectionFacts,
        alpn: Option<String>,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError>
    where
        S: zero_platform_tokio::ClientStream + InboundFallbackCapture + 'static,
        S::Stream: zero_platform_tokio::ClientStream + 'static,
    {
        let accepted = match self.profile.accept_standard_facts(&mut stream).await {
            Ok(accepted) => accepted,
            Err(error) => {
                if !self.fallback_enabled {
                    return Err(datagram::failure(error));
                }
                let (_, mut replay) = crate::inbound::VlessClientAcceptError::new(error, stream)
                    .into_fallback_replay();
                replay
                    .select_route(
                        &self.transport.fallback_policy,
                        facts.sni.as_deref(),
                        alpn.as_deref(),
                        facts.effective_source,
                        facts.local_endpoint,
                    )
                    .await
                    .map_err(datagram::failure)?;
                return Ok(AcceptedRequest::Fallback(fallback::accepted(replay)?));
            }
        };
        let (stream, recorded) = stream.into_fallback_replay_parts();
        let handshake_bytes = recorded.len() as u64;
        drop(recorded);
        if accepted.reverse_tag.is_some() {
            return Ok(AcceptedRequest::Control(self.profile.standard_reverse(
                stream,
                accepted,
                self.mux_response_backlog,
                &context,
            )?));
        }
        if accepted.request.command == crate::shared::CMD_MUX {
            return crate::mux::standard::source(
                stream,
                accepted.request.id,
                accepted.request.flow,
                accepted.testseed,
                accepted.auth,
                self.mux_response_backlog,
                &context,
                facts,
            )
            .await;
        }
        let target = accepted.request.destination;
        let request_facts = facts.request(
            &context,
            ProtocolType::new("vless"),
            Some(target.clone()),
            Some(accepted.auth),
        )?;
        if accepted.request.command == crate::shared::CMD_UDP {
            let sender = facts.effective_source.ok_or_else(|| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::InvalidConfig,
                    zero_platform_tokio::contract::Impact::Session,
                )
            })?;
            return Ok(AcceptedRequest::Datagram(accept_datagram(
                datagram::Reply {
                    stream: TcpRelayStream::new(stream),
                    facts: request_facts,
                    sender,
                    target,
                    handshake_bytes,
                },
            )));
        }
        if crate::flow::is_vision_flow(accepted.request.flow) {
            Ok(AcceptedRequest::Stream(accept_stream(
                crate::component::inbound::vision::Reply {
                    io: crate::inbound::vision::VlessInboundTcpStream::vision(
                        stream,
                        accepted.request.id,
                        accepted.testseed,
                    ),
                    facts: request_facts,
                },
            )))
        } else {
            Ok(AcceptedRequest::Stream(accept_stream(
                crate::component::inbound::TcpReply::new(stream, request_facts)?,
            )))
        }
    }
}
#[cfg(test)]
#[path = "../../../tests/component/inbound/mod.rs"]
mod standard_inbound_tests;
