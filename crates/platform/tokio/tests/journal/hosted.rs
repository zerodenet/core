use super::*;
use crate::contract::{lifecycle::TaskClass, scope::TaskScope};
use tokio::sync::oneshot;
use zero_core::component::{ComponentId, ComponentInstanceId, ResourceGeneration};
fn instance(value: u64) -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(value),
        generation: ResourceGeneration(1),
    }
}
#[tokio::test]
async fn one_graph_reservation_survives_native_close_and_real_hosted_registration_ack() {
    let directory = tempfile::tempdir().unwrap();
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let owner = JournalOwner::new_hosted(
        spec(directory.path().join("state"), 64),
        instance(97),
        tasks.retirement_observation(),
        CancellationToken::new(),
        &root,
    )
    .unwrap();
    assert_eq!(
        root.snapshot().charged,
        0,
        "cold declaration opens and charges nothing"
    );
    owner.start().unwrap();
    owner
        .grant()
        .replace(6, |write| write.write_all(b"first\n"))
        .unwrap();
    assert_eq!(
        root.snapshot().charged,
        1,
        "whole original graph, not its descriptor count"
    );
    let (finish, finished) = oneshot::channel();
    let (_, witness) = tasks
        .spawn_tracked(TaskClass::Critical, async move {
            finished.await.unwrap();
            Ok(())
        })
        .unwrap();
    let receipt = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&receipt));
    assert_eq!(
        root.snapshot().charged,
        1,
        "physical closure cannot refund an open Scope"
    );
    scope.begin_shutdown();
    finish.send(()).unwrap();
    let actual = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(actual.joined()));
    assert_eq!(
        root.snapshot().charged,
        1,
        "joined is independent of Host ACK"
    );
    scope.acknowledge(&actual).unwrap();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}
#[tokio::test]
async fn active_read_fences_late_calls_and_retains_same_native_even_when_external_owner_is_lost() {
    let directory = tempfile::tempdir().unwrap();
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let stop = CancellationToken::new();
    let owner = JournalOwner::new_hosted(
        spec(directory.path().join("state"), 64),
        instance(98),
        tasks.retirement_observation(),
        stop.clone(),
        &root,
    )
    .unwrap();
    owner.start().unwrap();
    let journal = owner.grant();
    journal
        .replace(5, |write| write.write_all(b"wire\n"))
        .unwrap();
    let (entered, entry) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let reader = journal.clone();
    let raw = tokio::task::spawn_blocking(move || {
        reader.read_snapshot(|read| {
            let mut byte = [0];
            read.read_exact(&mut byte)?;
            assert_eq!(byte, [b'w']);
            entered.send(()).unwrap();
            released.recv().unwrap();
            Ok(())
        })
    });
    let (_, witness) = tasks
        .spawn_tracked(TaskClass::Critical, async move {
            raw.await.unwrap().unwrap();
            Ok(())
        })
        .unwrap();
    entry.await.unwrap();
    assert!(
        owner.try_close().unwrap().is_none(),
        "the read has acquired its real original and remains active"
    );
    assert_eq!(root.snapshot().charged, 1);
    stop.cancel();
    drop(owner);
    assert_eq!(
        root.snapshot().charged,
        1,
        "Root retains the concrete controller, not a scalar reservation"
    );
    release.send(()).unwrap();
    scope.begin_shutdown();
    let actual = scope.next_receipt().await.unwrap();
    assert!(witness.matches_joined(actual.joined()));
    std::future::poll_fn(|cx| {
        root.poll_cancelled_retirements(cx);
        std::task::Poll::Ready(())
    })
    .await;
    assert!(journal.0.lock().closed.is_some());
    assert!(journal.append(1, |write| write.write_all(b"x")).is_err());
    assert_eq!(root.snapshot().charged, 1);
    scope.acknowledge(&actual).unwrap();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}

#[tokio::test]
async fn passive_host_observation_never_contends_with_single_active_io_call_invariant() {
    let directory = tempfile::tempdir().unwrap();
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let owner = Arc::new(
        JournalOwner::new_hosted(
            spec(directory.path().join("state"), 4096),
            instance(99),
            tasks.retirement_observation(),
            CancellationToken::new(),
            &root,
        )
        .unwrap(),
    );
    owner.start().unwrap();
    let journal = owner.grant();
    journal
        .replace(6, |write| write.write_all(b"seed!\n"))
        .unwrap();

    // Genuine concurrent IO on a second thread still fails with WouldBlock.
    let (entered, entry) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let active = journal.clone();
    let reader = std::thread::spawn(move || {
        active
            .read_snapshot(|read| {
                let mut byte = [0];
                read.read_exact(&mut byte)?;
                entered.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .unwrap();
    });
    entry.await.unwrap();
    let concurrent = journal
        .append(1, |write| write.write_all(b"x"))
        .unwrap_err();
    assert_eq!(concurrent.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(
        concurrent.to_string(),
        "journal already has its one actual IO call"
    );
    release.send(()).unwrap();
    reader.join().unwrap();

    // Tight passive Host/evidence polling on another OS thread must never
    // steal Core::state or cause serialized journal IO calls to WouldBlock.
    let done = Arc::new(AtomicBool::new(false));
    let observed_owner = owner.clone();
    let observed_grant = journal.clone();
    let observed_root = root.clone();
    let done_flag = done.clone();
    let observer = std::thread::spawn(move || {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        while !done_flag.load(Ordering::Acquire) {
            assert!(observed_owner.closed().is_none());
            assert!(observed_owner.failure().is_none());
            assert!(!observed_grant.native_closed());
            observed_root.poll_cancelled_retirements(&mut cx);
        }
    });

    for step in 0..1_000_usize {
        if step % 64 == 0 {
            journal
                .replace(6, |write| write.write_all(b"seed!\n"))
                .unwrap();
        } else {
            journal
                .append(4, |write| write.write_all(b"row\n"))
                .unwrap();
        }
        assert!(journal.current_bytes().unwrap() >= 6);
    }

    done.store(true, Ordering::Release);
    observer.join().unwrap();

    let receipt = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&receipt));
    assert!(journal.native_closed());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert_eq!(root.snapshot().charged, 0);
}
