use super::*;
use std::{
    error::Error,
    future::Future,
    task::{Context, Waker},
};
static LIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
#[ignore = "actual native ICMP access required; root explicitly runs loopback cases"]
async fn same_grant_owns_pending_original_and_actual_close_receipt_survives_retirement() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(CancellationToken::new());
    let grant = owner.grant();
    owner.start(deadline()).await.unwrap();
    let mut view = grant.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
    {
        let _held = owner.shared.state();
        let busy = grant.close(&view).err().unwrap();
        assert_eq!(busy.reason, Reason::Capacity);
        assert_eq!(busy.delivery, DeliveryState::NotSubmitted);
        assert!(
            view.local_addr().is_ok(),
            "reentrant/busy close cannot drop the original"
        );
    }
    let mut bytes = [0; 128];
    let mut pending = Box::pin(view.recv_from(&mut bytes));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    owner.revoke(CloseReason::Shutdown);
    assert!(pending.await.is_err());
    assert!(!owner.physical_confirmed());
    assert!(
        grant.close(&view).unwrap().matches(&view),
        "same pool retains cleanup authority after revoke"
    );
    assert!(view.local_addr().is_err());
    assert!(owner.physical_confirmed());
    assert_eq!(owner.take_closed_io().closed_io.len(), 1);
    assert!(owner.take_closed_io().closed_io.is_empty());
    assert!(owner
        .shutdown(deadline())
        .await
        .unwrap()
        .closed_io
        .is_empty());
}
#[tokio::test]
#[ignore = "actual native ICMP access required"]
async fn actual_closed_entry_recycles_single_slot_across_sixty_five_probes() {
    let _serial = LIVE.lock().await;
    let mut owner = owner(CancellationToken::new());
    owner.start(deadline()).await.unwrap();
    let grant = owner.grant();
    for _ in 0..65 {
        let view = grant.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
        assert!(view.local_addr().is_ok());
        assert!(grant.bind(Ipv4Addr::LOCALHOST.into(), None).is_err());
        assert!(grant.close(&view).unwrap().matches(&view));
    }
    let state = owner.shared.state();
    assert_eq!(state.closed_total, 65);
    assert_eq!(state.status.retained_live, 0);
    drop(state);
    let report = owner.shutdown(deadline()).await.unwrap();
    assert_eq!(
        report.closed_io.len(),
        1,
        "bounded aggregate represents all 65 actual receipts"
    );
    assert!(owner.physical_confirmed());
    assert!(owner.take_closed_io().closed_io.is_empty());
}
#[test]
#[ignore = "actual native ICMP access required before real dead-reactor registration"]
fn registration_unknown_retains_original_source_and_never_issues_empty_physical_success() {
    let _serial = LIVE.blocking_lock();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = owner(CancellationToken::new());
    runtime.block_on(owner.start(deadline())).unwrap();
    let grant = owner.grant();
    {
        let _entered = runtime.enter();
        let first = grant.bind(Ipv4Addr::LOCALHOST.into(), None).unwrap();
        assert!(grant.close(&first).unwrap().matches(&first));
    }
    let handle = runtime.handle().clone();
    drop(runtime);
    let grant = owner.grant();
    let failure = {
        let _entered = handle.enter();
        grant.bind(Ipv4Addr::LOCALHOST.into(), None).err().unwrap()
    };
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    let first_fault = owner.fault().unwrap();
    let source = owner.shared.state().status.unconfirmed[0].source.clone();
    let mut cause: &(dyn Error + 'static) = &failure;
    loop {
        if let Some(actual) = cause.downcast_ref::<std::io::Error>() {
            assert!(std::ptr::eq(actual, source.as_ref()));
            break;
        }
        cause = cause
            .source()
            .expect("actual original OS/registration error remains in source chain");
    }
    assert!(grant.bind(Ipv4Addr::LOCALHOST.into(), None).is_err());
    let cleanup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for retry in 0..2 {
        assert!(cleanup.block_on(owner.shutdown(deadline())).is_err());
        assert!(!owner.physical_confirmed());
        assert_eq!(
            owner.take_closed_io().closed_io.len(),
            usize::from(retry == 0),
            "actual earlier close remains available even when another original is Unknown"
        );
        assert!(Arc::ptr_eq(&first_fault, &owner.fault().unwrap()));
        assert!(Arc::ptr_eq(
            &source,
            &owner.shared.state().status.unconfirmed[0].source
        ));
    }
}
#[test]
#[ignore = "actual native ICMP access required; intentionally panic at real missing-reactor registration"]
fn post_create_unwind_cannot_reuse_cached_unused_pool_proof() {
    let _serial = LIVE.blocking_lock();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = owner(CancellationToken::new());
    runtime.block_on(owner.start(deadline())).unwrap();
    drop(runtime);
    let grant = owner.grant();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = grant.bind(Ipv4Addr::LOCALHOST.into(), None);
    }));
    assert!(panic.is_err(), "actual Tokio registration needs a reactor");
    owner.revoke(CloseReason::Shutdown);
    assert!(
        !owner.physical_confirmed(),
        "refresh the actual pool despite poisoned outer mutex"
    );
    assert_eq!(owner.shared.state().status.unconfirmed.len(), 1);
    assert_eq!(owner.shared.state().closed_total, 0);
    assert!(owner.take_closed_io().closed_io.is_empty());
}
