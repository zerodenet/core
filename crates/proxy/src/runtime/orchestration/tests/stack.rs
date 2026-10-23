//! Startup and recovery must fit the native Windows main-thread stack.
use super::*;

#[tokio::test]
async fn orchestration_boundaries_keep_large_phases_out_of_their_callers() {
    let proxy = Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    )
    .unwrap();
    fn check(name: &str, future: impl Future) {
        let bytes = std::mem::size_of_val(&future);
        assert!(
            bytes < 128 * 1024,
            "{name} embeds {bytes} bytes of runtime state"
        );
    }
    check("entry", run_until(&proxy, std::future::pending()));
    check(
        "supervision",
        run_supervised_until(&proxy, std::future::pending()),
    );
    let mut state = OrchestrationState::assemble(&proxy).unwrap();
    check(
        "run loop",
        run_loop(&proxy, &mut state, std::future::pending()),
    );
}

#[test]
fn management_runtime_reaches_ready_and_confirms_shutdown_on_a_one_mib_thread() {
    std::thread::Builder::new()
        .name("native-main-stack".into())
        .stack_size(1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let proxy = Proxy::new(
                        zero_config::RuntimeConfig::parse(
                            r#"{"route":{"final":{"type":"direct"}}}"#,
                        )
                        .unwrap(),
                    )
                    .unwrap();
                    let mut ready = proxy.orchestration_ready.subscribe();
                    let reached_ready = std::sync::atomic::AtomicBool::new(false);
                    let stop_after_actual_ready = async {
                        while !*ready.borrow_and_update() {
                            ready.changed().await.unwrap();
                        }
                        reached_ready.store(true, std::sync::atomic::Ordering::SeqCst);
                    };
                    tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        proxy.run_until(stop_after_actual_ready),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                    assert!(reached_ready.load(std::sync::atomic::Ordering::SeqCst));
                    assert!(!*proxy.orchestration_ready.borrow());
                });
        })
        .unwrap()
        .join()
        .unwrap();
}
