use super::*;
use std::{future::Future, num::NonZeroUsize, task::Poll};
use tokio::sync::oneshot;
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}

#[tokio::test]
async fn actual_host_process_budget_retains_pending_original_and_requires_tree_actor_and_native_closes(
) {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let id = ComponentInstanceId {
        component: ComponentId(731),
        generation: ResourceGeneration(1),
    };
    let resources = HostResources::new_for_instance(
        id,
        &[ResourceDeclaration::Process {
            program: "python3".into(),
            max_starts: NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let phase = resources.activate(end()).unwrap();
    let foreign = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    assert!(resources
        .prepare_owned_process(ProcessRequest::new("python3"), &foreign.registrar())
        .is_err());
    let mut request = ProcessRequest::new("python3");
    request.arguments = vec!["-c".into(), "import time;time.sleep(60)".into()];
    let original = resources
        .prepare_owned_process(request.clone(), &scope.registrar())
        .unwrap();
    assert_eq!(
        resources
            .prepare_owned_process(request, &scope.registrar())
            .unwrap_err()
            .reason,
        Reason::Capacity
    );
    assert!(!resources.io_evidence().processes[0].confirmed);
    let (begin, begun) = oneshot::channel();
    let (ready, pending) = oneshot::channel();
    let actual = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            actual.start(begun.await.unwrap()).unwrap();
            actual.wait_started(end()).await.unwrap();
            let mut stopped = Box::pin(actual.wait_tree_ended(end()));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(stopped.as_mut().poll(cx).is_pending()))
                    .await
            );
            ready.send(()).unwrap();
            stopped.await.unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    tokio::time::timeout_at(end(), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(original.closed().is_none());
    assert!(!witness.is_joined());
    drop(phase);
    let (native, tasks) = tokio::join!(resources.shutdown_io(end()), scope.shutdown(end()));
    let native = native.unwrap();
    assert!(native.is_complete(), "{native:?}");
    assert_eq!(native.closed_io.len(), 1);
    assert!(tasks.is_complete());
    assert_eq!(tasks.exited_tasks.len(), 1);
    assert!(witness.is_joined());
    assert!(original.closed().unwrap().matches(&original));
    let proof = resources.io_evidence();
    assert!(
        proof.admission_closed && proof.physical_confirmed(),
        "{proof:?}"
    );
    assert!(proof.sticky_faults.is_empty());
}

#[tokio::test]
async fn actual_unused_cold_process_grant_has_no_fabricated_native_or_actor() {
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let id = ComponentInstanceId {
        component: ComponentId(732),
        generation: ResourceGeneration(1),
    };
    let resources = HostResources::new_for_instance(
        id,
        &[ResourceDeclaration::Process {
            program: "python3".into(),
            max_starts: NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let phase = resources.activate(end()).unwrap();
    let original = resources
        .prepare_owned_process(ProcessRequest::new("python3"), &scope.registrar())
        .unwrap();
    assert!(original.root_status().is_none());
    assert!(original.try_finish().unwrap().is_none());
    drop(phase);
    assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
    assert!(original.closed().unwrap().matches(&original));
    assert!(original.root_status().is_none());
    assert!(resources.io_evidence().physical_confirmed());
}

#[tokio::test]
async fn failed_original_launch_keeps_the_same_fault_after_native_close_before_first_observation() {
    let directory = tempfile::tempdir().unwrap();
    let program = directory.path().join("missing-native-plugin");
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let id = ComponentInstanceId {
        component: ComponentId(733),
        generation: ResourceGeneration(1),
    };
    let resources = HostResources::new_for_instance(
        id,
        &[ResourceDeclaration::Process {
            program: program.clone(),
            max_starts: NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let phase = resources.activate(end()).unwrap();
    let original = resources
        .prepare_owned_process(ProcessRequest::new(program), &scope.registrar())
        .unwrap();
    let (begin, begun) = oneshot::channel();
    let (observed, failure) = oneshot::channel();
    let actual = original.clone();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let result = async {
                actual.start(begun.await.unwrap())?;
                actual.wait_started(end()).await
            }
            .await;
            assert!(
                result.is_err(),
                "actual OS launch, no synthetic readiness result"
            );
            observed
                .send(actual.first_failure().expect("same native launch fault"))
                .unwrap();
            Ok(())
        })
        .unwrap();
    begin.send(witness.clone()).unwrap();
    let native_failure = tokio::time::timeout_at(end(), failure)
        .await
        .unwrap()
        .unwrap();
    drop(phase);
    // Deliberately finish the same production native branch before sampling.
    // A matching Scope join and real provider receipt do not erase its fault.
    let (native, tasks) = tokio::join!(
        resources.processes.shutdown(id.component, end()),
        scope.shutdown(end()),
    );
    assert!(native.is_complete(), "{native:?}");
    assert!(tasks.is_complete() && witness.is_joined());
    let closed = original.closed().expect("same failed original closed");
    assert!(closed.matches(&original));
    #[cfg(target_os = "linux")]
    assert!(
        closed.had_native_acquisition(),
        "the actual helper and FDs closed"
    );
    let first = resources.io_evidence();
    let fault = first.processes[0]
        .first_fault
        .as_ref()
        .expect("fault survives physical close");
    assert!(first.processes[0].confirmed);
    use std::error::Error;
    let source = fault
        .source()
        .unwrap()
        .downcast_ref::<ProcessFailure>()
        .unwrap();
    assert!(Arc::ptr_eq(&source.0, &native_failure));
    let again = resources.io_evidence();
    assert!(Arc::ptr_eq(
        fault,
        again.processes[0].first_fault.as_ref().unwrap()
    ));
    assert!(again
        .sticky_faults
        .iter()
        .any(|source| Arc::ptr_eq(source, fault)));
}
