//! Execute a standard source using its original accepted entries and host Scope.
use crate::runtime::route_runtime::InboundListenerRuntime;
#[cfg(feature = "managed-datagram-runtime")]
mod peer;
use futures_util::{stream::FuturesUnordered, StreamExt};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    context::AcceptContext, inbound::DynInboundSource, lifecycle::TaskClass, CancellationToken,
    ChannelError, Impact, Reason, Stage,
};

pub(in crate::runtime) async fn run(
    mut source: Box<dyn DynInboundSource>,
    runtime: InboundListenerRuntime,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    limit: std::num::NonZeroUsize,
    protocol: &'static str,
) -> Result<(), EngineError> {
    let cancel = CancellationToken::new();
    let _stop = cancel.clone().drop_guard();
    let factory = runtime.route_factory();
    let mut closing = FuturesUnordered::new();
    let work = async {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            tokio::select! { biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                result = closing.next(), if !closing.is_empty() => {
                    if let Some(result) = result { result?; }
                }
                pending = source.next(AcceptContext { deadline: None, cancel: cancel.clone() }), if closing.len() < limit.get() => {
                    let Some(pending) = pending.map_err(std::io::Error::other)? else { return Ok(()); };
                    #[cfg(feature = "raw-ip-runtime")]
                    if pending.packet_listener().is_some() {
                        let context=runtime.take_packet_source().ok_or_else(||std::io::Error::other("packet source lacks actual original context"))?;
                        return super::raw_ip::run(pending,context,runtime.clone(),shutdown.clone()).await;
                    }
                    #[cfg(feature = "managed-datagram-runtime")]
                    if pending.carrier_facts().is_some() {
                        peer::spawn_carrier(&runtime,&factory,pending,cancel.clone())?;
                        continue;
                    }
                    #[cfg(feature = "managed-datagram-runtime")]
                    if pending.datagram_peer().is_some() {
                        peer::spawn(&runtime, &factory, pending, cancel.clone())?;
                        continue;
                    }
                    let (claim, receiver) = pending.accepted_tcp().ok_or_else(|| std::io::Error::other(
                        "TCP source omitted its original accepted entry"))?;
                    let facts = pending.connection_facts().cloned().ok_or_else(|| std::io::Error::other(
                        "TCP source omitted actual connection facts"))?;
                    let route = factory.for_connection(facts.effective_source).with_local_addr(facts.local_endpoint);
                    let cancel = cancel.clone();
                    let tag = factory.inbound_tag().to_owned();
                    let (_, witness) = pending.spawn(TaskClass::Auxiliary, move |pending, tasks| async move {
                        let route = route.with_accepted_tasks(tasks)?;
                        let result = route.complete_request(pending, cancel).await;
                        if let Err(error) = &result {
                            tracing::debug!(?error,"standard inbound accepted request failed");
                            crate::logging::log_listener_connection_error(
                                crate::logging::INBOUND_ACCEPT_ROUTE_STAGE, protocol, &tag,
                                &facts.effective_source, error);
                        }
                        result.map_err(|source| ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source))
                    }).map_err(std::io::Error::other)?;
                    closing.push(async move {
                        let _original_join = witness.wait().await;
                        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                        if receiver.close(&claim, deadline).await.map_err(std::io::Error::other)?.is_none() {
                            return Err(EngineError::Io(std::io::Error::other("accepted users remain unconfirmed")));
                        }
                        Ok(())
                    });
                }
            }
        }
    };
    // A pending first accept is readiness, never a fabricated native-close seal.
    let ready = if runtime.has_packet_source() {
        None
    } else {
        runtime.readiness()
    };
    crate::runtime::inbound_owners::InboundReady::poll_optional(ready, work).await
}
