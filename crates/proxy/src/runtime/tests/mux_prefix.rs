//! Actual bound protocol listener shared by internal migration regressions.
use super::Proxy;
use tokio::time::{Duration, Instant};
const ID: &str = "11111111-2222-3333-4444-555555555555";
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
pub(crate) struct MuxPrefix {
    pub node: zero_config::OutboundConfig,
    proxy: Proxy,
    stop: tokio::sync::watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), zero_engine::EngineError>>,
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
        let config=zero_config::RuntimeConfig::parse_with_source_dir(&serde_json::json!({"inbounds":[{"tag":"incoming","listen":{"address":"127.0.0.1","port":1},"protocol":inbound}],"route":{"final":{"type":"direct"}}}).to_string(),Some(material.path().to_owned())).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let startup = proxy.engine().runtime_snapshot().config().clone();
        proxy
            .protocols
            .start_components(startup.clone(), end())
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&startup, || Ok(()))
            .unwrap();
        let mut inbound = config.inbounds[0].clone();
        inbound.listen.port = 0;
        let listener = if protocol == "shadowsocks" {
            // This real source binds TCP and UDP on the same numeric port.
            // A TCP-only ephemeral bind does not reserve that UDP address.
            let reserved = reserve_tcp_udp();
            inbound.listen.port = reserved.0.local_addr().unwrap().port();
            crate::runtime::test_listener::start_with_handoff(&proxy, inbound, move || {
                drop(reserved)
            })
            .await
        } else {
            crate::runtime::test_listener::start(&proxy, inbound).await
        };
        let address = listener.address;
        let stop = listener.stop;
        let task = listener.worker;

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
            proxy,
            stop,
            task,
            _material: material,
        }
    }
    pub async fn close(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap().unwrap();
        self.proxy
            .inbound_owners
            .shutdown("incoming", end())
            .await
            .unwrap();
        self.proxy
            .protocols
            .shutdown_components(end())
            .await
            .unwrap();
    }
}

fn reserve_tcp_udp() -> (std::net::TcpListener, std::net::UdpSocket) {
    for _ in 0..16 {
        let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        match std::net::UdpSocket::bind(tcp.local_addr().unwrap()) {
            Ok(udp) => return (tcp, udp),
            Err(source) if source.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(source) => panic!("reserve actual mixed-prefix UDP address: {source}"),
        }
    }
    panic!("no shared TCP/UDP fixture address after 16 actual bind attempts")
}
