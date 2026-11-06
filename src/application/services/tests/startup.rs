use super::*;
use crate::application::services::{
    network::ApplicationEventSinkTcpDialer,
    startup::{ApplicationStartupSlot, StartupPause},
    StatusMonitor,
};
use std::sync::Arc;

async fn cancel_after_original_worker_is_installed(
    directory: &Path,
) -> (
    Arc<ApplicationStartupSlot>,
    Arc<ApplicationServices>,
    Engine,
) {
    let mut config = RuntimeConfig::parse(&config_json("startup.jsonl", 0)).unwrap();
    config.source_dir = Some(directory.to_owned());
    let engine = Engine::new(config).unwrap();
    let slot = Arc::new(ApplicationStartupSlot::new());
    let pause = Arc::new(StartupPause {
        prepared: tokio::sync::Notify::new(),
        resume: tokio::sync::Semaphore::new(0),
    });
    *slot.pause.lock().unwrap() = Some(pause.clone());
    let owner_slot = slot.clone();
    let candidate = engine.clone();
    let caller = tokio::spawn(async move {
        ApplicationServices::start_with_network(
            candidate,
            None,
            zero_connector::EventDispatcherNetwork::new(Arc::new(
                ApplicationEventSinkTcpDialer::new(
                    zero_platform_tokio::EgressInterfaceControl::default(),
                ),
            )),
            &owner_slot,
        )
        .await
        .map_err(|error| error.to_string())
    });
    tokio::time::timeout(Duration::from_secs(3), pause.prepared.notified())
        .await
        .unwrap();
    let original = slot
        .original()
        .expect("same original installed outside caller");
    caller.abort();
    assert!(matches!(caller.await, Err(error) if error.is_cancelled())); // caller only, not native proof
    assert!(Arc::ptr_eq(&original, &slot.original().unwrap()));
    assert!(original.state.lock().await.dispatcher.is_some());
    (slot, original, engine)
}

#[tokio::test]
async fn cancelled_initializer_keeps_original_worker_until_real_retirement() {
    let directory = tempfile::tempdir().unwrap();
    let (slot, original, _) = cancel_after_original_worker_is_installed(directory.path()).await;
    assert!(slot.install(original.clone()).is_err());
    {
        let mut state = original.state.lock().await;
        assert!(state
            .dispatcher
            .as_mut()
            .unwrap()
            .wait_ready()
            .await
            .unwrap());
        original.stop_active_services(&mut state).await.unwrap();
        assert!(state.dispatcher.is_none());
        assert!(!state.network_generation_started);
    }
    // Clear only after actual original worker joins and native generation close.
    slot.clear(&original);
    assert!(slot.original().is_none());
}

#[tokio::test]
async fn startup_unknown_cleanup_retains_exact_original_and_rejects_next_startup() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let directory = tempfile::tempdir().unwrap();
        let (slot, original, engine) =
            cancel_after_original_worker_is_installed(directory.path()).await;
        {
            let mut state = original.state.lock().await;
            state.status_monitor = Some(StatusMonitor::test_work(async {
                panic!("original startup monitor failed")
            }));
        }
        tokio::task::yield_now().await;
        {
            let mut state = original.state.lock().await;
            assert!(original.stop_active_services(&mut state).await.is_err());
            assert!(state.cleanup_fault.is_some());
            assert!(!state.network_generation_started); // distinct native close did complete
        }
        let next = ApplicationServices::start_with_network(
            engine.clone(),
            None,
            zero_connector::EventDispatcherNetwork::new(Arc::new(
                ApplicationEventSinkTcpDialer::new(
                    zero_platform_tokio::EgressInterfaceControl::default(),
                ),
            )),
            &slot,
        )
        .await;
        assert!(next.is_err());
        assert!(Arc::ptr_eq(&original, &slot.original().unwrap()));
        assert!(original.state.lock().await.cleanup_fault.is_some());
        // The panic is retained diagnostic history, distinct from unknown native
        // cleanup. Independently retire hooks/files/generation before any restart.
        assert!(original.shutdown_dispatcher().await.is_err());
        assert!(original.service_retirement_complete().await);
        original.shutdown_control().await.unwrap();
        assert!(original.control_retirement_complete());
        assert!(original.parent_input_retirement_complete());
        original.engine.shutdown_principal_quota_state().unwrap();
        assert!(original.engine.principal_quota_retirement_complete());
        slot.clear(&original);
        assert!(slot.original().is_none());
        let config = (*original.engine.config()).clone();
        // Only the cancelled initializer was supposed to stop at this test gate.
        // All original retirement assertions above precede a fresh real startup.
        *slot.pause.lock().unwrap() = None;
        let fresh = ApplicationServices::start_with_network(
            Engine::new(config).unwrap(),
            None,
            zero_connector::EventDispatcherNetwork::new(Arc::new(
                ApplicationEventSinkTcpDialer::new(
                    zero_platform_tokio::EgressInterfaceControl::default(),
                ),
            )),
            &slot,
        )
        .await
        .unwrap();
        fresh.shutdown_status_monitor().await.unwrap();
        fresh.shutdown_dispatcher().await.unwrap();
        assert!(fresh.service_retirement_complete().await);
        assert!(original.state.lock().await.cleanup_fault.is_some());
    })
    .await
    .unwrap();
}
