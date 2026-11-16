//! All ordinary and upper-pool users consume the same actual authenticated pool.
use super::*;
use zero_platform_tokio::{
    contract::context::{with_deadline, OpenContext},
    TcpRelayStream,
};
mod logical;
pub(in crate::component) async fn open(
    generation: &PacketMetadataArc<entry::Generation>,
    context: &OpenContext,
) -> Result<crate::component::preconnect::Opened, ChannelError> {
    let work = async {
        let pool = generation
            .hysteria
            .get()
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
        let binding = context.upstream.binding();
        let dns = generation.upstream.carrier_dns_generation();
        let epoch = pool.enter(binding, dns).await?;
        let slot = pool.slot(binding, &epoch)?;
        let _serial = slot.lock().await;
        let mut retried = false;
        let (io, origin) = loop {
            let cached = slot.cached();
            let from_cache = cached.is_some();
            let (client, origin) = match cached {
                Some(cached) => cached,
                None => {
                    slot.retire_original().await?;
                    if generation.upstream.carrier_dns_generation() != dns {
                        return Err(error(Stage::Prepare, Reason::Revoked));
                    }
                    entry::Generation::acquire_hysteria(generation, context, &slot)
                        .await?
                        .commit_hysteria(&slot)?
                }
            };
            match client.open().await {
                Ok(io) => {
                    drop(client);
                    break (io, origin);
                }
                Err(source) => {
                    // Local capacity/cancellation cannot justify replacing a
                    // healthy authenticated connection used by other callers.
                    let local = matches!(
                        source.reason,
                        Reason::Capacity | Reason::Cancelled | Reason::Revoked
                    );
                    drop(client);
                    if local {
                        return Err(source);
                    }
                    slot.request_retirement();
                    if !from_cache || retried {
                        return Err(source);
                    }
                    retried = true;
                    // Preserve the existing one transparent reconnect, but
                    // wait for the actual old native Root before rebuilding.
                    slot.retire_original().await?;
                }
            }
        };
        if generation.cancel.is_cancelled() || generation.upstream.carrier_dns_generation() != dns {
            drop(io);
            slot.request_retirement();
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let opened = logical::opened(generation, io, origin)?;
        Ok::<_, ChannelError>(opened)
    };
    let mut opened = tokio::select! { biased;
        _=generation.cancel.cancelled()=>return Err(error(Stage::Prepare, Reason::Revoked)),
        _=context.cancel.cancelled()=>return Err(error(Stage::Prepare, Reason::Cancelled)),
        result=with_deadline(context.deadline,work)=>result.map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))??,
    };
    if generation.options.carrier.is_none() {
        opened.io = tokio::select! { biased;
            _=generation.cancel.cancelled()=>return Err(error(Stage::Handshake, Reason::Revoked)),
            _=context.cancel.cancelled()=>return Err(error(Stage::Handshake, Reason::Cancelled)),
            result=with_deadline(context.deadline,generation.options.plan.encrypt(opened.io))=>result.map_err(|source|error(Stage::Handshake,Reason::Timeout).with_source(source))?
                .map_err(|source|error(Stage::Handshake,Reason::Io).with_source(source))?,
        };
    }
    Ok(opened)
}
