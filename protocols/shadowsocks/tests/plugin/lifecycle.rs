use super::*;
use crate::validation::PluginMode;
fn config() -> PluginConfig {
    PluginConfig {
        command: "python3".into(), options: None, mode: PluginMode::TcpOnly,
        args: vec!["-c".into(), "import os,socket,time; s=socket.socket(); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); s.listen(); time.sleep(60)".into()],
    }
}
#[tokio::test]
async fn plugin_pool_reload_keeps_active_leases_and_exit_is_not_silently_restarted() {
    let pool = PluginPool::default();
    let old = pool.plan("127.0.0.1", 9999, config());
    let lease = old.acquire(true).await.unwrap().unwrap();
    let same = pool
        .plan("127.0.0.1", 9999, config())
        .acquire(true)
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&lease, &same));
    pool.clear();
    lease.check().unwrap();
    let new = pool.plan("127.0.0.1", 9999, config());
    let replacement = new.acquire(true).await.unwrap().unwrap();
    assert!(!Arc::ptr_eq(&lease, &replacement));
    lease.child.lock().unwrap().start_kill().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while lease.check().is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(old.acquire(true).await.is_err());
    replacement.check().unwrap();
}

#[tokio::test]
async fn plugin_startup_failure_returns_an_error_without_direct_fallback() {
    let pool = PluginPool::default();
    let mut config = config();
    config.args = vec!["-c".into(), "raise SystemExit(7)".into()];
    assert!(pool
        .plan("127.0.0.1", 9999, config)
        .acquire(true)
        .await
        .is_err());
}

#[tokio::test]
async fn plugin_exit_closes_idle_udp_subscribers_without_another_packet() {
    let pool = PluginPool::default();
    let mut cfg = config();
    cfg.mode = PluginMode::UdpOnly;
    cfg.args[1] = "import os,socket,time; s=socket.socket(type=socket.SOCK_DGRAM); s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT']))); time.sleep(60)".into();
    let plan = pool.plan("127.0.0.1", 9999, cfg);
    let lease = plan.acquire(false).await.unwrap().unwrap();
    let factory = zero_transport::OutboundDatagramSocketFactory::new(Default::default());
    let codec = Arc::new(crate::udp::ShadowsocksDatagramCodec::new(
        crate::CipherKind::Aes128Gcm,
        "test",
    ));
    let flow = crate::transport::udp_socket::establish_with_plugin(
        lease.endpoint(),
        codec,
        &factory,
        Some(lease.clone()),
    )
    .await
    .unwrap();
    let mut received = flow.subscribe();
    lease.child.lock().unwrap().start_kill().unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .is_err()
    );
    assert!(flow
        .send_datagram(&zero_core::Address::Ipv4([127, 0, 0, 1]), 53, b"query")
        .await
        .is_err());
}

#[test]
fn plugin_cache_distinguishes_missing_options_from_an_explicit_empty_environment_value() {
    let pool = PluginPool::default();
    let absent = pool.plan("127.0.0.1", 9999, config());
    let mut empty = config();
    empty.options = Some(String::new());
    let empty = pool.plan("127.0.0.1", 9999, empty);
    assert_ne!(absent.cache_identity(), empty.cache_identity());
}
