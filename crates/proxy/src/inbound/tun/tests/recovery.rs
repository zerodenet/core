use crate::runtime::{Proxy, TunControl};

fn proxy() -> Proxy {
    Proxy::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn manual_recovery_requires_a_running_route_runtime() {
    assert!(proxy()
        .recover_tun()
        .await
        .unwrap_err()
        .to_string()
        .contains("automatic routes are not running"));
}

#[tokio::test]
async fn manual_recovery_waits_for_the_audit_and_returns_its_failure() {
    for outcome in [
        Ok(()),
        Err("physical default route is still unavailable".to_owned()),
    ] {
        let proxy = proxy();
        let (requests, mut receiver) = tokio::sync::mpsc::channel(1);
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let (_done, done) = tokio::sync::oneshot::channel();
        *proxy.tun_control.lock().unwrap() = Some(TunControl {
            id: 7,
            shutdown,
            done,
            route_done: None,
            route_recovery: Some(requests),
        });
        let recovery = proxy.recover_tun();
        tokio::pin!(recovery);
        let acknowledgement = tokio::select! {
            result = &mut recovery => panic!("returned before route audit: {result:?}"),
            request = receiver.recv() => request.unwrap(),
        };
        let expected = outcome.clone();
        acknowledgement.send(outcome).unwrap();
        let result = recovery.await;
        match expected {
            Ok(()) => result.unwrap(),
            Err(message) => assert!(result.unwrap_err().to_string().contains(&message)),
        }
        assert_eq!(proxy.tun_control.lock().unwrap().as_ref().unwrap().id, 7);
    }
}

#[test]
fn successful_shutdown_releases_a_previously_fail_closed_egress() {
    let control = zero_platform_tokio::EgressInterfaceControl::default();
    control.replace_tunnel_addresses(["10.66.0.1".parse().unwrap()]);
    control.mark_unavailable_for(false, "old interface disappeared");
    let peer = "192.0.2.1:443".parse().unwrap();
    assert!(control.select_for_peer(peer).ensure_connectable().is_err());
    super::super::clear_egress_after_route_cleanup(&control, true);
    assert!(!control.select_for_peer(peer).tun_active());
    control.select_for_peer(peer).ensure_connectable().unwrap();
}

#[tokio::test]
async fn explicit_stop_interrupts_recovery_without_waiting_for_its_timeout() {
    let proxy = proxy();
    let (requests, mut receiver) = tokio::sync::mpsc::channel(1);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (done_tx, done) = tokio::sync::oneshot::channel();
    let (route_tx, route_done) = tokio::sync::oneshot::channel();
    *proxy.tun_control.lock().unwrap() = Some(TunControl {
        id: 7,
        shutdown,
        done,
        route_done: Some(route_done),
        route_recovery: Some(requests),
    });
    let recovering = proxy.clone();
    let recovery = tokio::spawn(async move { recovering.recover_tun().await });
    let acknowledgement = receiver.recv().await.unwrap();
    let stopping = proxy.clone();
    let stop = tokio::spawn(async move { stopping.stop_tun().await });
    tokio::time::timeout(std::time::Duration::from_secs(1), shutdown_rx.changed())
        .await
        .expect("stop must interrupt a pending recovery")
        .unwrap();
    drop(acknowledgement);
    done_tx.send(()).unwrap();
    route_tx.send(Ok(())).unwrap();
    stop.await.unwrap().unwrap();
    assert!(recovery.await.unwrap().is_err());
    assert!(proxy.tun_control.lock().unwrap().is_none());
}

#[test]
fn generation_logs_distinguish_notifications_from_changed_route_policy() {
    // Tracing callsite interest is process-wide. Preserve all assertions in an
    // isolated copy of this test instead of depending on parallel subscribers.
    const CHILD: &str = "ZERO_TEST_TUN_GENERATION_LOG_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "inbound::tun::tests::recovery::generation_logs_distinguish_notifications_from_changed_route_policy",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed; 0 failed"),
            "isolated logging assertions failed:\n{}\n{}",
            stdout,
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use std::sync::{Arc, Mutex};
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let output = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .json()
        .with_writer(move || LogWriter(output.clone()))
        .finish();
    let control = zero_platform_tokio::EgressInterfaceControl::default();
    tracing::subscriber::with_default(subscriber, || {
        for (cause, changed) in [
            ("network_event", Some(false)),
            ("route_repair", Some(true)),
            ("tun_addresses_published", None),
        ] {
            let before = control.generation();
            control.invalidate_network();
            super::super::log_tun_generation_change(&control, before, cause, changed);
        }
        let unchanged = control.generation();
        super::super::log_tun_generation_change(&control, unchanged, "network_event", Some(false));
        assert_eq!(control.generation(), unchanged);
    });
    let bytes = buffer.lock().unwrap().clone();
    let text = String::from_utf8(bytes).unwrap();
    let events = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["fields"]["reconciliation_changed"], false);
    assert_eq!(events[1]["fields"]["reconciliation_changed"], true);
    assert!(events[2]["fields"].get("reconciliation_changed").is_none());
    for event in events {
        assert_eq!(
            event["fields"]["event_type"],
            "tun_network_generation_changed"
        );
    }
}

struct LogWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
