//! A raw QUIC connection retains its original sealed stream and actual TLS facts.
use super::*;
impl VlessInboundListenerRequest {
    pub async fn accept_standard_quic(
        self,
        connection: zero_transport::quic::owned::OwnedConnection,
        local: std::net::SocketAddr,
        context: HandshakeContext,
        resources: std::sync::Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
    ) -> Result<AcceptedRequest, ChannelError> {
        let metadata = connection.handshake_metadata();
        let source = connection.remote_address();
        let facts = InboundConnectionFacts {
            original_source: Some(source),
            effective_source: Some(source),
            local_endpoint: Some(local),
            sni: metadata.0,
        };
        if self.hysteria.is_some() || self.transport.split_http.is_some() {
            let caller = context.accepted.as_ref().ok_or_else(|| {
                ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Revoked,
                    zero_platform_tokio::contract::Impact::Session,
                )
            })?;
            let incoming = if let Some(profile) = self.hysteria.as_ref() {
                zero_transport::hysteria::accept_connection_scoped(
                    connection, profile, caller, resources,
                )
                .map_err(datagram::failure)?
            } else {
                zero_transport::split_http::accept_xhttp_h3_connection_scoped(
                    connection,
                    self.transport.split_http.as_ref().unwrap(),
                    self.transport.split_http_registry.as_ref().unwrap(),
                    caller,
                )
                .map_err(datagram::failure)?
            };
            let metadata = VlessInboundStreamMetadata {
                sni: facts.sni.clone(),
                alpn: metadata.1,
                source: facts.effective_source,
                destination: facts.local_endpoint,
            };
            return Ok(super::streams::from_native(
                self,
                VlessInboundTransportStreams {
                    incoming: super::super::multiplex::Incoming::Scoped(incoming),
                    metadata,
                },
                facts,
                context.cancel.child_token(),
            ));
        }
        let stream = connection.accept_bi_stream().await?;
        let stream = session::decrypt_stream(
            self.transport.decryption.as_ref(),
            TcpRelayStream::new(stream),
        )
        .await
        .map_err(datagram::failure)?;
        self.accept_standard_stream(record_client_stream(stream), facts, metadata.1, context)
            .await
    }
}
