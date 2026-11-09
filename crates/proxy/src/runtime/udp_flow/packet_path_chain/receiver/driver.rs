//! The same original association actor polls its actual retained RX futures.
use super::*;
pub(super) async fn run(shared: PacketMetadataArc<Shared>) -> Result<(), ChannelError> {
    let mut active = FuturesUnordered::new();
    loop {
        let changed = shared.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let mut pending = std::mem::take(
            &mut *shared
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        while !pending.is_empty() {
            active.push(pending.remove(0));
        }
        drop(pending);
        tokio::select! { biased;
            _=shared.stop.cancelled()=>break,
            _=changed=>{},
            Some(result)=active.next(),if !active.is_empty()=>{
                result?;
            }
        }
    }
    // Drop every original outstanding read/view outside locks. Their installed
    // logical controllers remain in RetiredDatagrams for actual closure.
    drop(active);
    let pending = std::mem::take(
        &mut *shared
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    drop(pending);
    if shared.failures.first.get().is_some() {
        Err(
            ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                .with_source(OriginalFailures(shared.failures.clone())),
        )
    } else {
        Ok(())
    }
}
