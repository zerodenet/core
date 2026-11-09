//! Convert the one actual endpoint connection; never start a library H3 task.
use super::*;
pub(super) async fn connected(
    generation: &Generation,
    entry: &Entry,
    connection: zero_transport::quic::owned::OwnedConnection,
) -> Result<Ready, ChannelError> {
    if generation.options.h3.is_none() {
        return Ok(Ready::Stream(zero_platform_tokio::TcpRelayStream::new(
            connection.into_bi_stream().await?,
        )));
    }
    let mut pending = {
        let mut state = entry.owners.state.lock().await;
        let mut prepared = state
            .h3
            .take()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let origin = entry
            .origin
            .get()
            .cloned()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        prepared.start(
            state
                .endpoint
                .as_ref()
                .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?,
            connection,
            entry.retirement.clone(),
            origin,
        )?
    };
    let client = pending.ready().await?;
    // The physical Entry and original driver are installed before publishing
    // this exact cached receiver. Confirmation never runs under the state gate.
    let receiver = {
        let state = entry.owners.state.lock().await;
        let endpoint = state
            .endpoint
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        match state
            .segment
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
        {
            entry::Segment::Native(segment) => endpoint.prepare_native_xhttp_receiver(
                &client,
                segment,
                state
                    .native_origin
                    .as_ref()
                    .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?,
                &entry.retirement,
            )?,
            entry::Segment::Borrowed(segment) => {
                endpoint.prepare_borrowed_xhttp_receiver(&client, segment, &entry.retirement)?
            }
        }
    };
    receiver.confirm()?;
    Ok(Ready::H3(client))
}
