//! The accepted half's direct material; no second leaf decode or XHTTP pool.
use super::*;
use zero_platform_tokio::{controlled_carrier::CarrierPreparation, TcpRelayStream};
pub(super) async fn authenticate(
    generation: &TcpGeneration,
    io: zero_platform_tokio::controlled_carrier::CarrierIo,
    preparation: CarrierPreparation,
    masks: zero_transport::finalmask::tcp::OwnedMasks,
) -> Result<TcpRelayStream, Failure> {
    let options = &generation.options;
    let io = masks
        .wrap(TcpRelayStream::new(io), false)
        .await
        .map_err(failure)?;
    if let Some(reality) = &options.protocol.reality {
        let mut staged = false;
        let staged_result = &mut staged;
        let stream = reality
            .connect_with_navigation(io, |stream, host, plan| async move {
                let grant = match preparation.reserve_job() {
                    Ok(grant) => grant,
                    Err(source) => {
                        tracing::debug!(%source, "original XHTTP navigation admission rejected");
                        return;
                    }
                };
                let Some(prepared) = zero_transport::http_navigation::prepare_owned(
                    stream,
                    host,
                    plan,
                    grant.clone(),
                ) else {
                    return;
                };
                let (delay, job) = prepared.into_parts();
                let work = Box::pin(async move {
                    job.await
                        .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))
                });
                match grant.stage(delay, work) {
                    Ok(()) => *staged_result = true,
                    Err(source) => {
                        tracing::debug!(%source, "same XHTTP original navigation staging rejected")
                    }
                }
            })
            .await
            .map_err(|source| Failure {
                source: error(Stage::Handshake, Reason::Io).with_source(source),
                staged_job: staged,
            })?;
        let bypass = stream.transport_bypass_control();
        return Ok(TcpRelayStream::with_transport_bypass_control(
            stream, bypass,
        ));
    }
    // TLS material was prepared by this actual owning Factory and is never
    // fetched through the ambiguous one-material-per-instance registry.
    match &options.protocol.material {
        Some(material) => material
            .prepare(generation.resolver.as_deref())
            .await
            .map_err(failure)?
            .connect(io)
            .await
            .map_err(failure),
        None => Ok(io),
    }
}
pub(super) struct Failure {
    pub source: ChannelError,
    pub staged_job: bool,
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> Failure {
    Failure {
        source: error(Stage::Handshake, Reason::Io).with_source(source),
        staged_job: false,
    }
}
