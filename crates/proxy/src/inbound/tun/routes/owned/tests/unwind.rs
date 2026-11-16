//! A real original worker panic cannot discard the acquired native monitor.
use super::*;
use std::{error::Error, num::NonZeroUsize, time::Duration};
use tokio::time::{timeout, Instant};
use zero_platform_tokio::contract::scope::TaskScope;
fn routes() -> OwnedRoutes {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    // No device/route mutation: this still acquires the real native notification
    // controller below, on the same prepaid original route responsibility.
    let start = crate::inbound::tun::managed::Start {
        name: None,
        raw_addr: String::new(),
        addresses: vec![],
        route_addresses: vec![],
        mtu: 1500,
        tag: "route-panic".into(),
        options: crate::inbound::tun::TunRuntimeOptions {
            auto_route: false,
            include_cidrs: vec![],
            exclude_cidrs: vec![],
            dual_stack: false,
            strict_route: false,
            dns_hijack: false,
        },
        managed_config: None,
        dns_hijacked_queries: Arc::default(),
    };
    let credit = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .driver_metadata_view()
        .reserve(OwnedRoutes::layout(&start).unwrap())
        .unwrap();
    let spec = RouteRuntimeSpec {
        id: 11,
        tun_name: String::new(),
        recovery_key: copy_key(&start.tag),
        primary_ipv6: false,
        addresses: copy_addresses(&start.route_addresses),
        include_cidrs: copy_prefixes(&start.options.include_cidrs),
        exclude_cidrs: copy_prefixes(&start.options.exclude_cidrs),
        dns_hijack: false,
        strict_route: false,
    };
    OwnedRoutes::from_prepaid(
        proxy,
        spec,
        Arc::default(),
        CancellationToken::new(),
        credit,
    )
    .unwrap()
}
#[tokio::test]
async fn original_route_worker_unwind_retains_native_cleanup_across_cancelled_wait_and_resumes_original_panic_in_scope(
) {
    timeout(Duration::from_secs(8), async {
        let mut actual = routes();
        actual
            .0
            .monitor_io
            .lock()
            .await
            .acquire()
            .expect("actual platform route-notification original");
        assert!(!actual.0.monitor_io.lock().await.is_closed());
        let (entered, arrived) = std::sync::mpsc::channel();
        let (resume, released) = std::sync::mpsc::channel();
        *actual.0.cleanup_gate.lock().unwrap() = Some((entered, released));
        // Ensure a failed assertion never strands the actual blocking child.
        struct Release(Option<std::sync::mpsc::Sender<()>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(resume) = self.0.take() {
                    let _ = resume.send(());
                }
            }
        }
        let mut release = Release(Some(resume));
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let original = actual.0.clone();
        let (id, witness) = scope
            .registrar()
            .spawn_blocking_tracked(TaskClass::Critical, move || worker::run(original))
            .unwrap();
        actual.0.worker.set(witness.clone()).unwrap();
        // A real earlier operation failure must survive a later panic; the
        // fixed prepaid diagnostic positions retain both original sources.
        let (completed, result) = oneshot::channel();
        actual
            .0
            .jobs
            .send(Job::Install("unprepared".into(), completed))
            .await
            .unwrap();
        let first = match result.await.unwrap() {
            Err(source) => source,
            Ok(_) => panic!("install without original preparation succeeded"),
        };
        actual.0.jobs.send(Job::Panic).await.unwrap();
        loop {
            match arrived.try_recv() {
                Ok(()) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                Err(source) => panic!("original worker gate: {source}"),
            }
        }
        assert!(actual.0.closing.load(Ordering::Acquire));
        assert!(actual.0.cancel.is_cancelled());
        // Poll the actual lifecycle wait while the original worker is explicitly
        // suspended before native close, then cancel this observer only.
        {
            let pending = actual.shutdown(Instant::now() + Duration::from_secs(3));
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
        }
        assert!(!witness.is_joined());
        assert!(!actual.0.monitor_io.lock().await.is_closed());
        assert!(!actual.0.observed.lock().unwrap().physically_closed);
        let failure = Arc::new(
            actual
                .shutdown(Instant::now() + Duration::from_millis(1))
                .await
                .unwrap_err(),
        );
        let mut source = Some(failure.as_ref() as &(dyn Error + 'static));
        let mut original_seen = false;
        let mut panic_seen = false;
        while let Some(current) = source {
            if let Some(io) = current.downcast_ref::<std::io::Error>() {
                original_seen |= std::ptr::eq(io, first.as_ref());
            }
            panic_seen |= current
                .to_string()
                .contains("original route responsibility unwound");
            source = current.source();
        }
        assert!(
            original_seen && panic_seen,
            "retain the actual earlier operation error and subsequent panic independently"
        );
        let retained = failure.retained_diagnostic().unwrap().clone();
        let receipt = failure.cleanup_failure_receipt().unwrap().clone();
        assert!(receipt.acknowledge_retained(&retained));
        assert!(
            !actual.0.monitor_io.lock().await.is_closed(),
            "diagnostic reception cannot close native IO"
        );
        let pending = actual
            .shutdown(Instant::now() + Duration::from_millis(1))
            .await
            .unwrap();
        assert!(!pending.is_complete());
        assert!(!witness.is_joined());
        release.0.take().unwrap().send(()).unwrap();
        let exit = scope.next_exit().await.unwrap();
        assert_eq!(exit.id, id);
        assert_eq!(exit.class, TaskClass::Critical);
        let error = exit.result.unwrap_err();
        let mut source = Some(&error as &(dyn Error + 'static));
        let mut panic = None;
        while let Some(current) = source {
            if let Some(native) = current.downcast_ref::<tokio::task::JoinError>() {
                panic = Some(native);
                break;
            }
            source = current.source();
        }
        let panic = panic.expect("actual original Tokio panic source retained");
        assert!(panic.is_panic());
        assert_eq!(Some(panic.id()), witness.native_task_id());
        let joined = witness.wait().await;
        assert_eq!(joined.id(), id);
        assert!(joined.result().is_err());
        assert!(
            actual.0.monitor_io.lock().await.is_closed(),
            "same original native controller positively closed"
        );
        assert!(actual.0.observed.lock().unwrap().physically_closed);
        assert!(actual.0.worker.get().unwrap().same_registration(&witness));
        assert!(actual
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap()
            .is_complete());
        scope.begin_shutdown();
        assert!(scope.next_exit().await.is_none());
        // Keep the original diagnostic payload and receipt after native closure.
        assert!(receipt.is_acknowledged());
        assert!(failure.retained_diagnostic().is_some());
    })
    .await
    .unwrap();
}
