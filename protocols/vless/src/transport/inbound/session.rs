use super::*;

pub(super) async fn accept_vless_stream_route<T, S, FWrap>(
    profile: crate::inbound::VlessInboundProfile,
    fallback_enabled: bool,
    fallback_policy: crate::fallback::FallbackPolicy,
    mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    stream: T,
    metadata: VlessInboundStreamMetadata,
    wrap_stream: FWrap,
) -> Result<
    zero_core::InboundRouteAccept<
        crate::inbound::VlessAcceptedClientRoute<S>,
        crate::inbound::VlessFallbackReplay<<S as zero_core::InboundFallbackCapture>::Stream>,
    >,
    RuntimeError,
>
where
    T: ClientStream + 'static,
    S: ClientStream + zero_core::InboundFallbackCapture + 'static,
    <S as zero_core::InboundFallbackCapture>::Stream: ClientStream + Send + 'static,
    FWrap: Fn(T) -> S + Clone + Send + 'static,
{
    let source = metadata.source.or_else(|| stream.peer_addr().ok());
    let destination = metadata.destination.or_else(|| stream.local_addr().ok());
    let sni = metadata.sni;
    match profile
        .clone()
        .accept_client_owned(crate::inbound::VlessInbound, wrap_stream(stream))
        .await
    {
        Ok(accepted) => match profile.prepare_reverse(
            accepted.with_inbound_endpoints(source, destination),
            mux_response_backlog,
        ) {
            Ok(control) => Ok(zero_core::InboundRouteAccept::Control(control)),
            Err(accepted) => accepted
                .into_route_with_sni(sni, mux_response_backlog)
                .await
                .map(zero_core::InboundRouteAccept::Route)
                .map_err(RuntimeError::from),
        },
        Err(rejected) => {
            let (auth_error, mut fallback_replay) = rejected.into_fallback_replay();
            if fallback_enabled {
                fallback_replay
                    .select_route(
                        &fallback_policy,
                        sni.as_deref(),
                        metadata.alpn.as_deref(),
                        source,
                        destination,
                    )
                    .await?;
                Ok(zero_core::InboundRouteAccept::Fallback(fallback_replay))
            } else {
                Err(RuntimeError::Core(auth_error))
            }
        }
    }
}

pub(super) async fn decrypt_stream(
    decryption: Option<&crate::encryption::EncryptionServer>,
    stream: TcpRelayStream,
) -> Result<TcpRelayStream, RuntimeError> {
    match decryption {
        Some(decryption) => Ok(decryption.handshake(stream).await?.into_relay()),
        None => Ok(stream),
    }
}
