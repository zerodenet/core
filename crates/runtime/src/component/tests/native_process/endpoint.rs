use super::*;
use std::num::NonZeroUsize;
use zero_core::component::ResourceGeneration;
use zero_platform_tokio::contract::component::ResourceAccess;
use zero_platform_tokio::{contract::scope::TaskScope, owned_process::ProcessEndpointSpec};
fn end() -> Instant {
    Instant::now() + std::time::Duration::from_secs(5)
}
#[tokio::test]
async fn host_process_endpoint_charges_same_job_before_native_reservation_and_keeps_failed_prechild_original()
 {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let foreign = TaskScope::empty(tokio::runtime::Handle::current());
    let id = ComponentInstanceId {
        component: ComponentId(733),
        generation: ResourceGeneration(1),
    };
    let resources = HostResources::new_for_instance(
        id,
        &[
            ResourceDeclaration::Process {
                program: "python3".into(),
                max_starts: NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::ProcessEndpoint {
                process_ordinal: 0,
                spec: ProcessEndpointSpec {
                    stream: true,
                    datagram: true,
                    ipv6: false,
                    network_generation: zero_core::component::NetworkGeneration(0),
                },
            },
        ],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    let (done, waiting) = tokio::sync::oneshot::channel();
    let (_, actor) = scope
        .registrar()
        .spawn_tracked(
            zero_platform_tokio::contract::scope::TaskClass::Critical,
            async move {
                waiting.await.unwrap();
                Ok(())
            },
        )
        .unwrap();
    let phase = resources.activate(end()).unwrap();
    let observation = resources.process_endpoint_retirement(1).unwrap();
    assert_eq!(observation.instance(), id);
    assert_eq!(observation.process_ordinal(), 0);
    assert!(observation.scope_identity().matches(&scope.registrar()));
    assert!(!observation.originals_closed());
    assert!(resources.process_endpoint_retirement(0).is_none());
    assert!(
        resources
            .reserve_process_endpoint(1, &foreign.registrar(), &actor)
            .is_err()
    );
    assert_eq!(resources.originals.process.snapshot().charged_jobs, 0);
    let endpoint = resources
        .reserve_process_endpoint(1, &scope.registrar(), &actor)
        .unwrap();
    assert_eq!(
        resources.originals.process.snapshot().charged_jobs,
        1,
        "reservation belongs to same already-charged original job"
    );
    assert!(
        resources
            .reserve_process_endpoint(1, &scope.registrar(), &actor)
            .is_err()
    );
    assert!(
        resources
            .prepare_owned_process(ProcessRequest::new("python3"), &scope.registrar())
            .is_err(),
        "bare prepare cannot bypass the declared handoff"
    );
    assert!(
        resources
            .prepare_owned_process_with_endpoint(
                ProcessRequest::new("not-the-declared-program"),
                &scope.registrar(),
                &endpoint
            )
            .is_err()
    );
    let prepared = resources
        .prepare_owned_process_with_endpoint(
            ProcessRequest::new("python3"),
            &scope.registrar(),
            &endpoint,
        )
        .unwrap();
    assert!(
        prepared.start(actor.clone()).is_err(),
        "missing exact reservation close cannot start the child"
    );
    assert!(prepared.root_status().is_none());
    assert!(!resources.io_evidence().physical_confirmed());
    assert!(!actor.is_joined());
    let pending_original = {
        resources
            .processes
            .lock()
            .get(&PathBuf::from("python3"))
            .unwrap()
            .jobs[0]
            .original
            .clone()
    };
    pending_original.request_stop();
    assert!(endpoint.shutdown(end()).await.unwrap().is_some());
    assert!(
        pending_original.try_finish().unwrap().is_none(),
        "real reservation IO cannot refund before original actor join"
    );
    assert_eq!(resources.originals.process.snapshot().charged_jobs, 1);
    assert!(!observation.originals_closed());
    done.send(()).unwrap();
    drop(phase);
    let (native, tasks) = tokio::join!(resources.shutdown_io(end()), scope.shutdown(end()));
    assert!(native.unwrap().is_complete());
    assert!(tasks.is_complete());
    assert!(actor.is_joined());
    assert!(endpoint.closed().unwrap().matches(&endpoint));
    assert!(resources.io_evidence().physical_confirmed());
    assert_eq!(resources.originals.process.snapshot().charged_jobs, 0);
    assert!(!pending_original.closed().unwrap().had_native_acquisition());
    assert!(observation.originals_closed());
    observation.wait_originals_closed(end()).await.unwrap();
    drop(resources);
    assert!(
        observation.originals_closed(),
        "positive original closure survives provider retirement"
    );
}

#[tokio::test]
async fn failed_endpoint_reservation_keeps_a_durable_original_observer_without_fabricating_actor_ack()
 {
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let id = ComponentInstanceId {
        component: ComponentId(734),
        generation: ResourceGeneration(1),
    };
    let resources = HostResources::new_for_instance(
        id,
        &[
            ResourceDeclaration::Process {
                program: "python3".into(),
                max_starts: NonZeroUsize::new(1).unwrap(),
            },
            ResourceDeclaration::ProcessEndpoint {
                process_ordinal: 0,
                spec: ProcessEndpointSpec {
                    stream: true,
                    datagram: true,
                    ipv6: false,
                    network_generation: zero_core::component::NetworkGeneration(0),
                },
            },
        ],
        CancellationToken::new(),
        scope.registrar().scope_identity(),
    )
    .unwrap();
    resources
        .originals
        .process
        .install(zero_platform_tokio::owned_process::ProcessBudgetLimits {
            max_jobs: 1,
            max_jobs_per_instance: 1,
        })
        .unwrap();
    let other = zero_platform_tokio::owned_process::OwnedProcess::prepare(
        ProcessRequest::new("python3"),
        &scope.registrar(),
    )
    .unwrap();
    resources
        .originals
        .process
        .reserve(
            ComponentInstanceId {
                component: ComponentId(735),
                generation: ResourceGeneration(1),
            },
            &other,
        )
        .unwrap();
    let (done, waiting) = tokio::sync::oneshot::channel();
    let (_, actor) = scope
        .registrar()
        .spawn_tracked(
            zero_platform_tokio::contract::scope::TaskClass::Critical,
            async move {
                waiting.await.unwrap();
                Ok(())
            },
        )
        .unwrap();
    let phase = resources.activate(end()).unwrap();
    let observation = resources.process_endpoint_retirement(1).unwrap();
    assert!(!observation.originals_closed());
    assert_eq!(
        resources
            .reserve_process_endpoint(1, &scope.registrar(), &actor)
            .err()
            .unwrap()
            .reason,
        Reason::Capacity
    );
    assert!(
        !observation.originals_closed(),
        "a failed call is no original closure"
    );
    drop(phase);
    assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
    assert!(
        observation.originals_closed(),
        "the actual provider closed its retained never-started originals"
    );
    assert!(
        !actor.is_joined(),
        "original observer does not supply its actor join or ACK"
    );
    done.send(()).unwrap();
    assert!(scope.shutdown(end()).await.is_complete());
    assert!(actor.try_joined().unwrap().registration_released());
    other.request_stop();
    assert!(
        !other
            .try_finish()
            .unwrap()
            .unwrap()
            .had_native_acquisition()
    );
    assert_eq!(resources.originals.process.snapshot().charged_jobs, 0);
    drop(resources);
    assert!(observation.originals_closed());
}
