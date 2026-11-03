//! Standard production source, actual native controls, real Child and Scope.
#![cfg(unix)]
use crate::runtime::{inbound_owners::NativeListenerView, test_listener};
use crate::Proxy;
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
};
use tokio::{
    sync::watch,
    time::{timeout, Duration, Instant},
};
fn config(root: &Path, mode: &str) -> zero_config::RuntimeConfig {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../protocols/shadowsocks/tests/support/sip003.py");
    zero_config::RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"ss","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"original","plugin":{"command":"python3","options":"test=options;escaped=value","mode":mode,"args":[fixture,"server",mode,root.join("child.json")]}}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap()
}
async fn ready_proxy(config: zero_config::RuntimeConfig) -> Proxy {
    let proxy = Proxy::new(config).unwrap();
    let config = proxy.engine().config();
    proxy
        .protocols
        .start_components(config.clone(), Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    proxy
}
async fn record(path: PathBuf) -> serde_json::Value {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(bytes) = std::fs::read(&path) {
                if let Ok(record) = serde_json::from_slice(&bytes) {
                    return record;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn standard_sip003_modes_confirm_originals_and_reap_before_same_port_replacement() {
    timeout(Duration::from_secs(25), async {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("zero_proxy=debug")
            .with_test_writer()
            .try_init();
        for mode in ["tcp_only", "udp_only", "tcp_and_udp"] {
            let root = tempfile::tempdir().unwrap();
            let proxy = ready_proxy(config(root.path(), mode)).await;
            let input = proxy.engine().config().inbounds[0].clone();
            let running = test_listener::start(&proxy, input.clone()).await;
            let address = running.address;
            let first = record(root.path().join("child.json")).await;
            let pid = first["pid"].as_u64().unwrap();
            assert_eq!(
                first["remote"][1].as_u64().unwrap(),
                u64::from(address.port())
            );
            assert!(proxy.inbound_owners.open_bind_admission().is_err());
            test_listener::close(&proxy, "ss", running).await;
            assert!(
                !pid_alive(pid as i32),
                "original plugin child must be reaped"
            );
            std::fs::remove_file(root.path().join("child.json")).unwrap();
            let mut next = input;
            next.listen.port = address.port();
            let running = test_listener::start(&proxy, next).await;
            let second = record(root.path().join("child.json")).await;
            assert_ne!(second["pid"], first["pid"]);
            test_listener::close(&proxy, "ss", running).await;
            proxy
                .protocols
                .shutdown_components(Instant::now() + Duration::from_secs(3))
                .await
                .unwrap();
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn cancellation_at_actual_pending_probe_retains_child_and_native_close_owners() {
    pending_start(false).await;
}
#[tokio::test]
async fn child_exit_at_actual_pending_probe_is_joined_and_originals_retire() {
    pending_start(true).await;
}
async fn pending_start(kill: bool) {
    timeout(Duration::from_secs(20), async {
        let root = tempfile::tempdir().unwrap();
        let proxy = ready_proxy(config(root.path(), "tcp_only")).await;
        let config = proxy.engine().config();
        let prepared = proxy
            .protocols
            .prepare_inbound_bind(config.inbounds[0].clone(), None)
            .unwrap()
            .unwrap();
        let (stop, _) = watch::channel(false);
        let mut lease = proxy
            .inbound_owners
            .reserve_listener(
                "ss",
                &config,
                &proxy.protocols,
                prepared,
                stop.clone(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        lease.resolve_native().await.unwrap();
        lease.bind_native().unwrap();
        let (definition, view, installed) = lease.finish().unwrap().promote_definition().unwrap();
        let NativeListenerView::Handoff { ref grant, .. } = view else {
            panic!("actual handoff source")
        };
        let gate = grant.pause_probe();
        let owner = installed.owner();
        let recipe = definition;
        let factory = crate::runtime::route_runtime::InboundListenerRuntimeFactory::new(
            crate::runtime::route_runtime::SharedIngressRuntimeServices::new(
                proxy.tcp_runtime_services(),
            ),
            proxy.inbound_owners.clone(),
        );
        let upstream = factory
            .for_inbound("ss".into())
            .route_factory()
            .for_connection(None)
            .upstream_services();
        let (source, peers) = installed
            .build_inbound(recipe, view, Some(upstream))
            .unwrap();
        let runtime = factory
            .for_inbound("ss".into())
            .with_inbound_instance(owner.identity())
            .with_readiness(owner.readiness())
            .with_datagram_source(peers);
        owner
            .spawn_driver(crate::runtime::inbound_operation::source::run(
                source,
                runtime,
                stop.subscribe(),
                NonZeroUsize::new(4096).unwrap(),
                "shadowsocks",
            ))
            .unwrap();
        owner.seal_listener_installation();
        let observed = owner.clone();
        let worker = tokio::spawn(async move { observed.observe_listener().await });
        gate.entered().await;
        let original = record(root.path().join("child.json")).await;
        let pid = original["pid"].as_u64().unwrap() as i32;
        if kill {
            assert!(std::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .unwrap()
                .success());
        } else {
            owner.task_access().request_stop();
            stop.send_replace(true);
        }
        let _actual_listener_outcome = worker.await.unwrap();
        let receipt = proxy
            .inbound_owners
            .confirm_shutdown("ss", Instant::now() + Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert!(
            receipt.terminal().closed_io_receipts() >= 3,
            "public reservation, kernel listener/UDP and actual probe originals"
        );
        assert!(receipt.terminal().joined_tasks() >= 2);
        assert!(!pid_alive(pid));
        assert!(proxy.inbound_owners.open_bind_admission().is_err());
        proxy
            .inbound_owners
            .release_closed(receipt, |_| Ok(()))
            .unwrap();
        proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
    })
    .await
    .unwrap();
}

fn pid_alive(pid: i32) -> bool {
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap()
        .success()
}
