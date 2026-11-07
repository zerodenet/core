//! One protocol-owned REALITY policy across actual TCP and installed UDP MUX.
use super::outbound::OwnedVlessOutboundTransportPlan;
use zero_transport::{RuntimeError, TcpRelayStream};
pub(super) async fn open_carrier(
    transport: &OwnedVlessOutboundTransportPlan,
    pool: &crate::mux_pool::MuxConnectionPool,
    reality: Option<&crate::reality::PreparedRealityClient>,
    io: TcpRelayStream,
    preparation: Option<zero_platform_tokio::controlled_carrier::CarrierPreparation>,
) -> Result<TcpRelayStream, RuntimeError> {
    if let Some(reality) = reality {
        // Only the owning recipe installs this cold, already-validated
        // policy. The same raw CarrierIo supplied this preparation token.
        let preparation = preparation.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "same original REALITY preparation is unavailable",
            )
        })?;
        let stream = reality
            .connect_with_navigation(io, move |stream, host, plan| async move {
                match preparation.reserve_job() {
                    Ok(grant) => crate::component::navigation::stage(stream, host, plan, grant),
                    Err(source) => {
                        tracing::debug!(%source, "original MUX navigation admission rejected")
                    }
                }
            })
            .await?;
        let control = stream.transport_bypass_control();
        return transport
            .encrypt(zero_transport::TcpRelayStream::with_transport_bypass_control(stream, control))
            .await;
    }
    if pool.native_stream_dependency()?.is_some() {
        // TLS, H2 and the gRPC body belong to the lower real cache. Keep
        // VLESS's own optional encryption above that exact logical view.
        transport.encrypt(io).await
    } else {
        transport
            .open_owned_mux_carrier(io, pool.native_instance()?)
            .await
    }
}
