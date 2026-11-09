use super::*;
pub(super) struct Remote {
    pub address: std::net::SocketAddr,
    kind: &'static str,
    running: crate::RunningProxy,
    proxy: Proxy,
    original: Arc<crate::runtime::inbound_owners::InboundOwner>,
}
impl Remote {
    pub async fn new(kind: &'static str) -> Self {
        let protocol = if kind == "shadowsocks" {
            serde_json::json!({"type":kind,"cipher":"aes-128-gcm","password":"actual-native-dns"})
        } else {
            serde_json::json!({"type":kind})
        };
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"remote","listen":{"address":"127.0.0.1","port":0},"protocol":protocol}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let registry = proxy.system_tun_source_registry();
        let running = proxy.spawn();
        let original = tokio::time::timeout_at(deadline(), async {
            loop {
                if let Some(owner) = registry.owner("remote") {
                    return owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout_at(deadline(), original.wait_ready())
            .await
            .unwrap()
            .unwrap();
        crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        )
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
        Self {
            address: *original.original_listener_address.get().unwrap(),
            kind,
            running,
            proxy,
            original,
        }
    }
    pub fn diagnose(&self) {
        eprintln!("actual remote Source: kind={} address={:?} running={} terminal={:?} activeSessions={} completedSessions={}",self.kind,self.address,self.original.is_running(),self.original.terminal_for_test(),self.proxy.engine().active_sessions().len(),self.proxy.engine().completed_sessions().len());
        self.original.diagnose_datagram_failures_for_test();
        eprintln!(
            "actual remote Root: {:?}",
            self.proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
        );
    }
    pub async fn assert_original_process_lookup(&self, expected: usize) {
        if self.kind != "shadowsocks" {
            return;
        }
        let peers = self.original.peer_tasks_for_test();
        tokio::time::timeout_at(deadline(), async {
            while peers.retired_process_lookup_count_for_test() < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(self.original.is_running());
        assert!(self.original.terminal_for_test().is_none());
    }
    pub async fn close(self) {
        self.running.shutdown().await.unwrap();
        let terminal = self.original.terminal_for_test().unwrap();
        assert_eq!(terminal.identity(), self.original.identity());
        assert!(
            terminal.failure().is_none()
                && terminal.task_failures().is_empty()
                && terminal.native_bound_failures().is_empty()
        );
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() > 0);
    }
}
#[derive(Debug)]
struct NoCompatibility;
impl zero_dns::DnsOutboundConnector for NoCompatibility {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async {
            panic!("native Datagram DNS cannot use a compatibility Stream connector")
        })
    }
}
pub(super) async fn client(
    remote: &Remote,
    dns: &crate::dns_fixture::Dns,
    tcp_only_plugin: bool,
) -> (Proxy, crate::RunningProxy, Option<tempfile::TempDir>) {
    let mut protocol =
        serde_json::json!({"type":remote.kind,"server":"127.0.0.1","port":remote.address.port()});
    if remote.kind == "shadowsocks" {
        protocol["cipher"] = "aes-128-gcm".into();
        protocol["password"] = "actual-native-dns".into();
    }
    let material = if tcp_only_plugin {
        let directory = tempfile::tempdir().unwrap();
        let helper = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../protocols/shadowsocks/tests/support/sip003.py");
        protocol["plugin"] = serde_json::json!({"command":"python3","args":[helper,"client","tcp_only",directory.path().join("must-not-start.json")],"options":"test=options;escaped=value","mode":"tcp_only"});
        Some(directory)
    } else {
        None
    };
    let mut backend = serde_json::to_value(dns.server()).unwrap();
    backend["detour"] = "node".into();
    // SOCKS has two distinct real originals (control TCP plus negotiated UDP).
    // Their actual declaration must hold both; the Query itself still has N1.
    let sockets = if remote.kind == "socks5" { 2 } else { 1 };
    let config=zero_config::RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"native_stream_max_originals":sockets,"native_dns_max_originals":1,"dns":{"servers":{"node":{"type":"system"},"query":backend},"default_server":"query","policy":{"node_server":"node","address_family":"ipv4_only"}}},
        "outbounds":[{"tag":"node","protocol":protocol}],"route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    proxy
        .resolver
        .set_outbound_connector(Arc::new(NoCompatibility));
    let running = proxy.spawn();
    crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    )
    .apply_runtime_config_and_wait(config, Duration::from_secs(5))
    .await
    .unwrap();
    (proxy, running, material)
}
