use super::*;
use std::{
    error::Error,
    future::Future,
    task::{Context, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};
use zero_core::{
    channel::{DeliveryState, RetryClass},
    component::{ComponentId, SessionId},
};

#[tokio::test]
async fn weak_registration_keeps_original_native_error_arc_fields_budget_and_ids() {
    let mut owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let weak = owner.registrar().downgrade();
    assert_eq!(Arc::strong_count(&owner.0), 1);
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native = Arc::new(std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err());
    let raw = native.raw_os_error().unwrap();
    let mut error =
        ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(native.clone());
    error.component = Some(ComponentId(203));
    error.session = Some(SessionId(79));
    error.delivery = DeliveryState::NotSubmitted;
    error.retry = RetryClass::Revalidate;
    let (id, witness) = weak
        .spawn_tracked(TaskClass::Auxiliary, async move { Err(error) })
        .unwrap();
    assert_eq!(Arc::strong_count(&owner.0), 1);
    assert_eq!(
        weak.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.id, id);
    assert_eq!(exit.class, TaskClass::Auxiliary);
    let forwarded = exit.result.unwrap_err();
    let original = witness.wait().await.result().unwrap_err();
    assert_eq!(forwarded.component, original.component);
    assert_eq!(forwarded.session, original.session);
    assert_eq!(forwarded.stage, original.stage);
    assert_eq!(forwarded.reason, original.reason);
    assert_eq!(forwarded.impact, original.impact);
    assert_eq!(forwarded.delivery, original.delivery);
    assert_eq!(forwarded.retry, original.retry);
    assert!(std::ptr::eq(
        forwarded
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ChannelError>()
            .unwrap(),
        original.as_ref()
    ));
    let retained = original
        .source()
        .unwrap()
        .downcast_ref::<Arc<std::io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(retained, &native));
    assert_eq!(retained.raw_os_error(), Some(raw));
    let next = weak.spawn(TaskClass::Critical, async { Ok(()) }).unwrap();
    assert!(next.0 > id.0);
    let report = owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, [next]);
    assert_eq!(
        weak.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
}

#[tokio::test]
async fn weak_registration_cannot_bypass_suspended_hosted_start_or_receipt_credit() {
    let mut owner = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(1));
    let weak = owner.registrar().downgrade();
    assert_eq!(
        weak.spawn(TaskClass::Auxiliary, async {
            panic!("cold body was polled")
        }),
        Err(SpawnError::NotStarted)
    );
    owner.enable_registration().unwrap();
    let (mut io, mut peer) = tokio::io::duplex(1);
    let (entered, observed) = oneshot::channel();
    let (id, witness) = weak
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let read = io.read_u8();
            tokio::pin!(read);
            let mut entered = Some(entered);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .unwrap();
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    assert!(owner
        .poll_exit(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert!(!witness.is_joined());
    assert_eq!(
        weak.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    peer.write_u8(1).await.unwrap();
    let receipt = owner.next_receipt().await.unwrap();
    assert_eq!(receipt.id(), id);
    assert_eq!(witness.wait().await.id(), id);
    assert_eq!(
        weak.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    owner.acknowledge(&receipt).unwrap();
    let next = weak.spawn(TaskClass::Auxiliary, async { Ok(()) }).unwrap();
    assert!(next.0 > id.0);
    let next = owner.next_receipt().await.unwrap();
    owner.acknowledge(&next).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
    assert_eq!(
        weak.spawn_tracked(TaskClass::Critical, async { Ok(()) })
            .unwrap_err(),
        SpawnError::Closing
    );
}

struct BodyDropped(Option<oneshot::Sender<()>>);
impl Drop for BodyDropped {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}

#[tokio::test]
async fn weak_authority_in_a_real_pending_task_does_not_retain_its_scope_or_fabricate_a_join() {
    let owner = TaskScope::new(Handle::current(), NonZeroUsize::new(1).unwrap());
    let weak = owner.registrar().downgrade();
    let in_task = weak.clone();
    let (mut io, _peer) = tokio::io::duplex(1);
    let (entered, observed) = oneshot::channel();
    let (dropped, destroyed) = oneshot::channel();
    let (_id, witness) = weak
        .spawn_tracked(TaskClass::Auxiliary, async move {
            let _authority = in_task;
            let _drop = BodyDropped(Some(dropped));
            let read = io.read_u8();
            tokio::pin!(read);
            let mut entered = Some(entered);
            poll_fn(|cx| {
                let result = read.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = entered.take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await
            .unwrap();
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    assert_eq!(Arc::strong_count(&owner.0), 1);
    assert!(!witness.is_joined());
    drop(owner); // Emergency cancellation/abort is not a native join receipt.
    assert!(
        weak.0.upgrade().is_none(),
        "no Scope→task→strong Registrar cycle"
    );
    assert_eq!(
        weak.spawn(TaskClass::Critical, async {
            panic!("expired authority polled")
        }),
        Err(SpawnError::Closing)
    );
    tokio::time::timeout(Duration::from_secs(1), destroyed)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !witness.is_joined(),
        "task body Drop never completes a join witness"
    );
}

mod callbacks;

mod cancellation;
