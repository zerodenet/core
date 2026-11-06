use super::*;
use crate::contract::{
    component::ProcessRequest,
    scope::{TaskClass, TaskScope},
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Poll, Wake, Waker},
};
use tokio::{sync::oneshot, time::Instant};
use zero_core::component::{ComponentId, ResourceGeneration};
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}
#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn actual_process_credit_waits_for_native_tree_actor_join_and_actual_deferred_host_ack() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let budget = ProcessBudget::new(ProcessBudgetLimits {
        max_jobs: 1,
        max_jobs_per_instance: 1,
    })
    .unwrap();
    let instance = ComponentInstanceId {
        component: ComponentId(901),
        generation: ResourceGeneration(1),
    };
    #[cfg(target_os = "linux")]
    let request = {
        let mut request = ProcessRequest::new("python3");
        request.arguments = vec!["-c".into(), "import time;time.sleep(60)".into()];
        request
    };
    #[cfg(windows)]
    let request = {
        let mut request = ProcessRequest::new("powershell.exe");
        request.arguments = vec![
            "-NoProfile".into(),
            "-NonInteractive".into(),
            "-Command".into(),
            "Start-Sleep -Seconds 60".into(),
        ];
        request
    };
    let original = OwnedProcess::prepare(request, &scope.registrar()).unwrap();
    budget.reserve(instance, &original).unwrap();
    let (begin, begun) = oneshot::channel();
    let (ready, started) = oneshot::channel();
    let actual = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            actual.start(begun.await.unwrap()).unwrap();
            actual.wait_started(end()).await.unwrap();
            ready.send(()).unwrap();
            actual.wait_tree_ended(end()).await.unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    tokio::time::timeout_at(end(), started)
        .await
        .unwrap()
        .unwrap();
    original.request_stop();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.result().is_ok());
    assert!(witness.matches_joined(receipt.joined()));
    assert!(original.try_finish().unwrap().unwrap().matches(&original));
    assert!(!receipt.joined().registration_released());
    assert_eq!(budget.snapshot().charged_jobs, 1);
    let successor =
        OwnedProcess::prepare(ProcessRequest::new("python3"), &scope.registrar()).unwrap();
    assert_eq!(
        budget.reserve(instance, &successor).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let (poison, payload) = panic::watch(&budget);
    let thrown = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        budget.poll_released(&mut Context::from_waker(&poison));
    }))
    .expect_err("actual original ACK Waker clone panicked");
    assert!(Arc::ptr_eq(
        thrown.downcast_ref::<Arc<usize>>().unwrap(),
        &payload
    ));
    let original_entry = budget.entries().into_iter().next().unwrap().1;
    assert!(!original_entry.polling.load(Ordering::Acquire));
    assert!(
        original_entry.acknowledged.lock().unwrap().is_none(),
        "never repoll the panicked future"
    );
    assert_eq!(budget.snapshot().charged_jobs, 1);
    assert!(original.closed().unwrap().matches(&original));
    assert!(!receipt.joined().registration_released());
    let wakes = Arc::new(Wakes::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    budget.poll_released(&mut cx);
    let callbacks = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert!(!receipt.joined().registration_released());
    assert_eq!(budget.snapshot().charged_jobs, 1);
    drop(callbacks);
    assert!(receipt.joined().registration_released());
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "actual ACK wakes the existing root observer"
    );
    budget.poll_released(&mut cx);
    assert_eq!(budget.snapshot().charged_jobs, 0);
    budget.reserve(instance, &successor).unwrap();
    successor.request_stop();
    let closed = successor.try_finish().unwrap().unwrap();
    assert!(!closed.had_native_acquisition());
    assert_eq!(budget.snapshot().charged_jobs, 0);
    scope.begin_shutdown();
    assert!(std::future::poll_fn(|cx| Poll::Ready(scope.poll_exit(cx).is_ready())).await);
    assert!(scope.is_drained());
}

mod panic;
