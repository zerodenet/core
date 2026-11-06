use super::*;
use crate::application::services::StatusMonitor;
use zero_proxy::ConfigApplyReconciler;

#[tokio::test]
async fn normal_sampler_replacement_keeps_supervision_on_the_next_published_scope() {
    tokio::time::timeout(Duration::from_secs(5), async {
    use std::{future::Future, task::Poll};
    let directory = tempfile::tempdir().unwrap();
    let mut config = RuntimeConfig::parse(&config_json("before.jsonl", 0)).unwrap();
    config.source_dir = Some(directory.path().to_owned());
    let services = ApplicationServices::start(Engine::new(config.clone()).unwrap(), None)
        .await
        .unwrap();
    let old = services
        .state
        .lock()
        .await
        .status_monitor
        .as_ref()
        .unwrap()
        .clone();
    let mut supervision = Box::pin(services.wait_status_failure());
    std::future::poll_fn(|cx| {
        assert!(supervision.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    config.api.event_sinks = RuntimeConfig::parse(&config_json("after.jsonl", 0))
        .unwrap()
        .api
        .event_sinks;
    tokio::select! {
        result = &mut supervision => panic!("old sampler's normal replacement ended application supervision: {result:?}"),
        result = services.reconcile(std::sync::Arc::new(config)) => { result.unwrap(); },
    }
    assert!(old.retirement_complete());
    let current = services
        .state
        .lock()
        .await
        .status_monitor
        .as_ref()
        .unwrap()
        .clone();
    assert_ne!(old.id(), current.id());
    std::future::poll_fn(|cx| {
        assert!(supervision.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(supervision);
    services.shutdown_status_monitor().await.unwrap();
    services.shutdown_dispatcher().await.unwrap();
    assert!(current.retirement_complete());
    assert!(services.service_retirement_complete().await);
    }).await.unwrap();
}

#[tokio::test]
async fn dispatcher_replacement_recovers_facts_after_storage_backpressure() {
    let dir = tempfile::tempdir().unwrap();
    let events = dir.path().join("events.jsonl");
    let mut config=RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[],"outbounds":[],"route":{"final":{"type":"direct"}},
        "api":{"event_sinks":[{"type":"jsonl","tag":"audit","path":events,"events":["engine.warning"]}],
            "outbox_path":dir.path().join("outbox.jsonl"),
            "dispatcher":{"max_in_memory_deliveries":1,"replay_batch_size":1,"outbox_min_free_bytes":u64::MAX,"outbox_min_free_percent":1}}
    }).to_string()).unwrap();
    let engine = Engine::new(config.clone()).unwrap();
    let services = ApplicationServices::start(engine.clone(), None)
        .await
        .unwrap();
    for index in 0..24 {
        engine.emit_warning(&format!("recover-{index}"), "fact before reload");
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = services.state.lock().await;
            let status = state.dispatcher.as_ref().unwrap().sink_status();
            if status[0].last_error.is_some() {
                break;
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    config.api.dispatcher.outbox_min_free_bytes = 1;
    // Keep a valid percentage reserve, but do not let the production 5%
    // default mask recovery of this fixture's deliberately blocked PUTs.
    // A failed replacement must not consume the old continuation. The next
    // reconciliation uses it instead of adopting a fresh snapshot watermark.
    let mut failed = config.clone();
    let blocked_path = dir.path().join("directory-not-a-file");
    std::fs::create_dir(&blocked_path).unwrap();
    if let zero_config::EventSinkConfig::JsonLines { path, .. } = &mut failed.api.event_sinks[0] {
        *path = blocked_path.display().to_string();
    }
    assert!(services
        .reconcile(std::sync::Arc::new(failed.clone()))
        .await
        .is_err());

    let original_preparation = services
        .state
        .lock()
        .await
        .dispatcher
        .as_ref()
        .unwrap()
        .original_preparation_failure()
        .unwrap();

    services
        .retire_candidate(std::sync::Arc::new(failed))
        .await
        .unwrap();
    {
        let state = services.state.lock().await;
        assert!(state.dispatcher.is_none() && !state.network_generation_started);
        assert!(
            state.cleanup_fault.is_none(),
            "already reported preparation is not unfinished cleanup"
        );
        let archive = state
            .cleanup_receiver(super::super::CleanupOwner::DispatcherClosed)
            .unwrap();
        let failure = archive
            .as_ref()
            .downcast_ref::<zero_connector::ConnectorError>()
            .unwrap();
        assert!(
            matches!(failure, zero_connector::ConnectorError::DispatcherRetirement(failure)
            if failure.is_only_preparation(&original_preparation))
        );
    }
    let config = std::sync::Arc::new(config);
    tokio::time::timeout(Duration::from_secs(2), services.reconcile(config.clone()))
        .await
        .unwrap()
        .unwrap();
    services.committed(config).await.unwrap();
    wait_for_file_text(&events, "recover-23").await;
    let content = std::fs::read_to_string(&events).unwrap();
    let rows = content
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 24);
    for index in 0..24 {
        assert_eq!(
            rows.iter()
                .filter(|row| row["payload"]["code"] == format!("recover-{index}"))
                .count(),
            1
        );
    }
    services.shutdown_status_monitor().await.unwrap();
    services.shutdown_dispatcher().await.unwrap();
}

#[tokio::test]
async fn monitor_original_join_failure_is_retained_and_blocks_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let config = RuntimeConfig::parse(&config_json("first.jsonl", 0)).unwrap();
    let mut config = config;
    config.source_dir = Some(dir.path().to_owned());
    let engine = Engine::new(config.clone()).unwrap();
    let services = ApplicationServices::start(engine, None).await.unwrap();
    {
        let mut state = services.state.lock().await;
        let original = state.status_monitor.as_ref().unwrap().clone();
        ApplicationServices::stop_status_monitor(&mut state)
            .await
            .unwrap();
        assert!(matches!(state.monitor_exit, Some(Ok(()))));
        assert!(
            original.retirement_complete(),
            "same cooperative worker and scope really joined"
        );
        state.monitor_exit = None;
        state.status_monitor = Some(StatusMonitor::test_work(async {
            panic!("actual original monitor failure")
        }));
    }
    // Let the real original task run to its own terminal panic; no injected
    // success, timeout or abort is used as the join acknowledgement.
    tokio::task::yield_now().await;
    assert!(services.shutdown_status_monitor().await.is_err());
    let failure = {
        let state = services.state.lock().await;
        match state.monitor_exit.as_ref().unwrap() {
            Err(error) => error.clone(),
            Ok(()) => panic!("original monitor panic was lost"),
        }
    };
    let committed = config.clone();
    config.api.event_sinks.clear();
    assert!(services
        .validate(&committed, &config)
        .unwrap_err()
        .contains("original application cleanup"));
    assert!(services
        .reconcile(std::sync::Arc::new(config))
        .await
        .is_err());
    assert!(services.shutdown_status_monitor().await.is_err());
    let state = services.state.lock().await;
    assert!(
        matches!(state.monitor_exit.as_ref(), Some(Err(error)) if std::sync::Arc::ptr_eq(&failure, error))
    );
    assert!(state.cleanup_fault.is_some());
    drop(state);
    // Even after a monitor fault, independently drive the real dispatcher and
    // native obligations; this does not clear the sticky monitor failure.
    assert!(services.shutdown_dispatcher().await.is_err());
    assert!(!services.state.lock().await.network_generation_started);
}

#[tokio::test]
async fn original_monitor_join_fault_is_not_replaced_by_an_earlier_cleanup_fault() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = RuntimeConfig::parse(&config_json("monitor-source.jsonl", 0)).unwrap();
    config.source_dir = Some(dir.path().to_owned());
    let services = ApplicationServices::start(Engine::new(config).unwrap(), None)
        .await
        .unwrap();
    // Two actual independently consumed task failures, not constructed JoinError
    // values or a String marker standing in for a native/Scope close outcome.
    let earlier = std::sync::Arc::new(
        tokio::task::spawn_blocking(|| panic!("actual earlier cleanup task"))
            .await
            .unwrap_err(),
    );
    let retained: std::sync::Arc<dyn std::error::Error + Send + Sync> = earlier.clone();
    {
        let mut state = services.state.lock().await;
        ApplicationServices::stop_status_monitor(&mut state)
            .await
            .unwrap();
        state.monitor_exit = None;
        state.cleanup_fault = Some(retained.clone());
        state.status_monitor = Some(StatusMonitor::test_work(async {
            panic!("actual later status monitor")
        }));
    }
    tokio::task::yield_now().await;
    let error = services.shutdown_status_monitor().await.unwrap_err();
    let actual = {
        let state = services.state.lock().await;
        assert!(
            state.status_monitor.is_none(),
            "the real original join was consumed"
        );
        assert!(std::sync::Arc::ptr_eq(
            state.cleanup_fault.as_ref().unwrap(),
            &retained
        ));
        state
            .monitor_exit
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .clone()
    };
    let reported = error
        .get_ref()
        .unwrap()
        .downcast_ref::<std::sync::Arc<zero_platform_tokio::contract::ChannelError>>()
        .expect("shutdown returns the actual original monitor source");
    assert!(std::sync::Arc::ptr_eq(reported, &actual));
    let source = std::error::Error::source(actual.as_ref())
        .unwrap()
        .downcast_ref::<tokio::task::JoinError>()
        .expect("same original sampler's actual panic join remains diagnostic");
    assert!(source.is_panic());
    assert_ne!(source.id(), earlier.id());
    // The separate dispatcher cleanup still executes all real native/worker
    // cleanup. Its one returned receiver must retain BOTH actual original
    // task failures after the old application state is released.
    let combined = services.shutdown_dispatcher().await.unwrap_err();
    assert!(!services.state.lock().await.network_generation_started);
    assert!(services.service_retirement_complete().await);
    services.shutdown_control().await.unwrap();
    services.engine.shutdown_principal_quota_state().unwrap();
    let original_monitor = std::sync::Arc::downgrade(&actual);
    let original_earlier = std::sync::Arc::downgrade(&earlier);
    drop(actual);
    drop(retained);
    drop(earlier);
    drop(error);
    drop(services);
    assert!(
        original_monitor.upgrade().is_some(),
        "secondary original must survive in the returned cleanup receiver"
    );
    assert!(original_earlier.upgrade().is_some());
    drop(combined);
    assert!(original_monitor.upgrade().is_none());
    assert!(original_earlier.upgrade().is_none());
}
