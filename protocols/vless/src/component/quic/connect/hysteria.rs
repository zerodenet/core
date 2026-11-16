//! Authentication and real H3 receiver confirmation precede cache publication.
use super::*;
pub(super) async fn connected(
    generation: &Generation,
    entry: &Entry,
    connection: zero_transport::quic::owned::OwnedConnection,
) -> Result<Ready, ChannelError> {
    let memory = generation.memory(
        PacketMetadataArc::<zero_transport::hysteria::owned::OwnedClient>::layout()
            .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
    )?;
    let mut pending = {
        let state = entry.owners.state.lock().await;
        let owner = state
            .endpoint
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        zero_transport::hysteria::owned::OwnedClient::prepare(
            owner,
            connection,
            generation
                .options
                .hysteria
                .as_ref()
                .expect("actual Hysteria profile")
                .0
                .clone(),
            entry.retirement.clone(),
        )?
    };
    let client = pending.ready().await?;
    let receiver = {
        let state = entry.owners.state.lock().await;
        let owner = state
            .endpoint
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        match state
            .segment
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
        {
            entry::Segment::Native(segment) => owner.prepare_native_hysteria_receiver(
                &client,
                segment,
                state
                    .native_origin
                    .as_ref()
                    .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?,
                &entry.retirement,
            )?,
            entry::Segment::Borrowed(segment) => {
                owner.prepare_borrowed_hysteria_receiver(&client, segment, &entry.retirement)?
            }
        }
    };
    receiver.confirm()?;
    let client = PacketMetadataArc::from_prepaid(client, memory)
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    Ok(Ready::Hysteria(client))
}
