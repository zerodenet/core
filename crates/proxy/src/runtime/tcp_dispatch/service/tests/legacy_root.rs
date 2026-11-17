//! Explicit raw root for standalone unit fixtures only.
use super::*;
pub(super) fn open<'a>(
    root: &'a RootOpener,
    binding: &UpstreamBinding,
    destination: Destination,
    context: CarrierContext,
) -> BoxFuture<'a, Result<BoxStream, ChannelError>> {
    let binding = binding.clone();
    Box::pin(async move {
        if binding != root.binding
            || !binding.prefix.is_empty()
            || destination != root.endpoint
            || context.session.is_some()
        {
            return Err(super::super::failure(Reason::StalePlan));
        }
        let socket = root
            .services
            .open_root_stream(&binding, destination, root.role, context)
            .await?;
        let local = socket
            .local_addr()
            .map_err(|source| super::super::failure(Reason::Io).with_source(source))?;
        let peer = socket
            .peer_addr()
            .map_err(|source| super::super::failure(Reason::Io).with_source(source))?;
        *root
            .evidence
            .lock()
            .expect("DNS service root evidence poisoned") = Some(Evidence {
            _local: local,
            _peer: peer,
        });
        Ok(Box::pin(socket) as BoxStream)
    })
}
