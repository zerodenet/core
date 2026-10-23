//! A public Engine reload before spawn must bootstrap that exact generation.
use crate::runtime::Proxy;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
    time::{timeout, Duration},
};

fn config(count: usize, dns_port: u16) -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":(0..count).map(|index|serde_json::json!({"tag":format!("node-{index}"),"protocol":{"type":"direct"}})).collect::<Vec<_>>(),
        "runtime":{"dns":{"servers":{"selected":{"type":"udp","host":"127.0.0.1","port":dns_port}},
            "default_server":"selected","policy":{"address_family":"ipv4_only"}}},
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}
#[tokio::test]
async fn startup_uses_the_same_captured_engine_generation_for_components_and_real_dns() {
    let old_dns = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let new_dns = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(config(3, old_dns.local_addr().unwrap().port())).unwrap();
    proxy
        .engine()
        .reload_runtime_config(config(35, new_dns.local_addr().unwrap().port()))
        .unwrap();
    let selected = proxy.engine().runtime_snapshot();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let dns = tokio::spawn(async move {
        let mut bytes = [0; 4096];
        let (size, peer) = new_dns.recv_from(&mut bytes).await.unwrap();
        let question = zero_dns::udp::parse_dns_question(&bytes[..size]).unwrap();
        assert_eq!(question.domain, "selected-startup.test");
        let reply = zero_dns::udp::build_dns_response(
            &bytes[..size],
            &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
        );
        new_dns.send_to(&reply, peer).await.unwrap();
    });
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 1024];
        assert!(socket.read(&mut bytes).await.unwrap() > 0);
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    });
    let running = proxy.spawn();
    let mut ready = proxy.orchestration_ready.subscribe();
    timeout(Duration::from_secs(2), async {
        let _ = ready.wait_for(|ready| *ready).await.unwrap();
    })
    .await
    .unwrap();
    let plan = proxy
        .protocols
        .current_components(selected.config())
        .expect("commit must use the actual started snapshot Arc");
    assert!(plan.configured("node-34").is_some());
    proxy
        .probe_outbound_single(
            "node-34",
            &format!("http://selected-startup.test:{port}/generation"),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), dns).await.unwrap().unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    let mut bytes = [0; 1];
    assert!(
        timeout(Duration::from_millis(20), old_dns.recv_from(&mut bytes))
            .await
            .is_err()
    );
    running.shutdown().await.unwrap();
}
