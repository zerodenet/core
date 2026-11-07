//! Real UDP is held by this one exact installed original control without an observer.
use super::*;
use crate::{
    contract::{
        component::GrantedMetadataListener,
        lifecycle::{CloseReport, DynChannelControl, Unfinished},
        BoxFuture, CloseReason,
    },
    controlled_datagram::{metadata::MetadataViewClaim, metadata_pool::MetadataPoolPermit},
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};
#[derive(Default)]
struct Gate {
    released: AtomicBool,
    panic_first: AtomicBool,
    late_io: Mutex<Option<std::path::PathBuf>>,
    io_source: Mutex<Option<Arc<ChannelError>>>,
    entered: AtomicUsize,
    changed: tokio::sync::Notify,
}
impl Gate {
    async fn wait(&self) {
        self.entered.fetch_add(1, Ordering::AcqRel);
        if self.panic_first.swap(false, Ordering::AcqRel) {
            std::panic::panic_any(ActualPollFailure);
        }
        // Exercise a real synchronous wake from inside the original poll.
        std::future::poll_fn(|cx| {
            cx.waker().wake_by_ref();
            Poll::Ready(())
        })
        .await;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}
struct Retained {
    _owner: Arc<MetadataPoolOwner>,
    gate: Arc<Gate>,
    permit: Arc<MetadataPoolPermit>,
    tasks: crate::contract::scope::TaskRegistrar,
    wire: Mutex<Option<GrantedMetadataListener>>,
    claim: Mutex<Option<MetadataViewClaim>>,
}
struct Control(Arc<Retained>);
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {}
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.0.gate.wait().await;
            if let Some(path) = self.0.gate.late_io.lock().unwrap().take() {
                let error = std::fs::File::open(path)
                    .expect_err("actual missing original path must fail in OS");
                let source = Arc::new(
                    ChannelError::new(Stage::Close, Reason::Io, crate::contract::Impact::Component)
                        .with_source(error),
                );
                *self.0.gate.io_source.lock().unwrap() = Some(source.clone());
                return Err(ChannelError::retained(source));
            }
            let wire = self.0.wire.lock().unwrap().take();
            if let Some(mut wire) = wire {
                let claim = wire.users_claim.take();
                drop(wire);
                *self.0.claim.lock().unwrap() = claim;
            }
            let mut claim = self.0.claim.lock().unwrap().take();
            if claim
                .as_ref()
                .is_some_and(|claim| claim.try_closed().is_none())
            {
                *self.0.claim.lock().unwrap() = claim;
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::Io("actual UDP view remains live".into())],
                    ..Default::default()
                });
            }
            self.0.permit.return_unbound(&mut claim, &self.0.tasks)?;
            Ok(CloseReport {
                released_resources: vec!["actual unique ancillary views returned".into()],
                ..Default::default()
            })
        })
    }
}

#[derive(Debug)]
struct ActualPollFailure;
#[path = "phase/fixture.rs"]
mod fixture;
#[tokio::test]
async fn exact_pre_io_control_survives_owner_loss_and_root_closes_real_udp_before_deferred_actor_ack(
) {
    fixture::run(false, false).await;
}
#[tokio::test]
async fn actual_original_poll_panic_retains_typed_payload_and_recovers_same_udp_before_real_host_ack(
) {
    fixture::run(true, false).await;
}

#[tokio::test]
async fn actual_panic_then_independent_os_cleanup_source_remain_in_same_receiver_until_native_and_ack(
) {
    fixture::run(true, true).await;
}
