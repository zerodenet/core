//! Actual bound protocol listener shared by internal migration regressions.
use super::Proxy;
use tokio::time::{Duration, Instant};
const ID: &str = "11111111-2222-3333-4444-555555555555";
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
pub(crate) struct MuxPrefix {
    pub node: zero_config::OutboundConfig,
    running: crate::RunningProxy,
    original: std::sync::Arc<crate::runtime::inbound_owners::InboundOwner>,
    _material: tempfile::TempDir,
}
impl MuxPrefix {
    pub async fn start(protocol: &str, tag: &str) -> Self {
        Self::start_profile(protocol, tag, None).await
    }
    pub async fn shadowsocks(cipher: &str, tag: &str) -> Self {
        Self::start_profile("shadowsocks", tag, Some(cipher)).await
    }
    async fn start_profile(protocol: &str, tag: &str, cipher: Option<&str>) -> Self {
        let material = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(material.path().join("ca.pem"), cert.cert.pem()).unwrap();
        std::fs::write(
            material.path().join("key.pem"),
            cert.signing_key.serialize_pem(),
        )
        .unwrap();
        let user = if protocol == "shadowsocks" {
            serde_json::json!({"cipher":cipher.unwrap(),"password":if cipher.unwrap().starts_with("2022") {"MDEyMzQ1Njc4OWFiY2RlZg=="} else {"ordinary-mixed-password"}})
        } else if protocol == "trojan" {
            serde_json::json!({"password":"mux-password"})
        } else {
            serde_json::json!({"id":ID})
        };
        let mut inbound = if protocol == "shadowsocks" {
            user.clone()
        } else {
            serde_json::json!({"users":[user.clone()],"tls":{"cert_path":"ca.pem","key_path":"key.pem"},"ws":{"path":"/mux"}})
        };
        inbound["type"] = protocol.into();
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({"inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":0},"protocol":inbound}],"route":{"final":{"type":"direct"}}}).to_string(),Some(material.path().to_owned())).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let registry = proxy.inbound_owners.clone();
        let running = proxy.spawn();
        let original = tokio::time::timeout_at(end(), async {
            let owner = loop {
                if let Some(owner) = registry.owner("incoming") {
                    break owner;
                }
                tokio::task::yield_now().await;
            };
            owner.wait_ready().await.unwrap();
            owner
        })
        .await
        .unwrap();
        let address = *original
            .original_listener_address
            .get()
            .expect("same actual Source bound its listener");

        let mut outbound = user;
        outbound["type"] = protocol.into();
        outbound["server"] = "127.0.0.1".into();
        outbound["port"] = address.port().into();
        if protocol != "shadowsocks" {
            outbound["mux_concurrency"] = 8.into();
            outbound["ws"] = serde_json::json!({"path":"/mux"});
        }
        if protocol == "trojan" {
            outbound["sni"] = "localhost".into();
            outbound["insecure"] = true.into();
        } else if protocol != "shadowsocks" {
            outbound["tls"] = serde_json::json!({"server_name":"localhost","insecure":true});
        }
        let node =
            serde_json::from_value(serde_json::json!({"tag":tag,"protocol":outbound})).unwrap();
        Self {
            node,
            running,
            original,
            _material: material,
        }
    }
    pub async fn close(self) {
        self.running.shutdown().await.unwrap();
        let terminal = self
            .original
            .terminal_for_test()
            .expect("same Source reached its actual terminal cleanup");
        assert!(terminal.failure().is_none(), "{:?}", terminal.failure());
        assert!(terminal.joined_tasks() > 0);
        assert!(terminal.closed_io_receipts() > 0);
    }
}
