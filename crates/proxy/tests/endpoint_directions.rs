#![cfg(all(feature = "wireguard", feature = "socks5"))]

#[path = "support/host.rs"]
mod host;
mod support;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::interop::socks5_udp_echo_to;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_api::{
    CommandRequest, CommandService, EndpointDirections, EndpointGetQuery, EndpointPersistence,
    EndpointSetDirectionsCommand, EndpointSetStateCommand, QueryRequest, QueryResponse,
    QueryService,
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[tokio::test]
async fn live_inbound_contraction_preserves_outbound_replies_and_ends_inbound_business() {
    let host = host::non_loopback_host_ipv4();
    let a_wire = free_udp_port();
    let b_wire = free_udp_port();
    let a_socks = free_port();
    let b_socks = free_port();
    let config = |private: u8, remote_private: u8, listen, remote, socks, address, inbound| {
        RuntimeConfig::parse(&json!({
            "endpoints":[{"tag":"wg", "directions":{"inbound":inbound,"outbound":true},
                "listen":{"address":"127.0.0.1","port":listen}, "protocol":{
                    "type":"wireguard", "private_key":STANDARD.encode([private;32]),
                    "addresses":[address], "peers":[{
                        "public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([remote_private;32])).as_bytes()),
                        "endpoint":format!("127.0.0.1:{remote}"), "allowed_ips":["10.0.0.0/24", format!("{host}/32")],
                        "keepalive_secs":1}]}}],
            "inbounds":[{"tag":"socks", "listen":{"address":"127.0.0.1","port":socks}, "protocol":{"type":"socks5"}}],
            "route":{"rules":[{"condition":{"type":"inbound","values":["socks"]},
                "action":{"type":"route","outbound":"wg"}}], "final":{"type":"direct"}}
        }).to_string()).unwrap()
    };
    let a_proxy = Proxy::new(config(31, 32, a_wire, b_wire, a_socks, "10.0.0.1/32", true)).unwrap();
    let a_handle = ProxyHandle::new(EngineHandle::new(a_proxy.engine().clone()), a_proxy.clone());
    let b_proxy = Proxy::new(config(32, 31, b_wire, a_wire, b_socks, "10.0.0.2/32", true)).unwrap();
    let b_handle = ProxyHandle::new(EngineHandle::new(b_proxy.engine().clone()), b_proxy.clone());
    let b = spawn_engine(b_proxy);
    let a = spawn_engine(a_proxy);
    wait_for_listener(a_socks).await;
    wait_for_listener(b_socks).await;
    let echo = UdpSocket::bind((host, 0)).await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let received = Arc::new(AtomicUsize::new(0));
    let count = received.clone();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0; 2048];
        loop {
            let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
            count.fetch_add(1, Ordering::Relaxed);
            echo.send_to(&buffer[..size], source).await.unwrap();
        }
    });
    let target = Address::Ipv4(host.octets());
    let reply = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(a_socks, target.clone(), echo_port, b"outbound-reply"),
    )
    .await
    .expect("outbound reply");
    assert_eq!(reply, b"outbound-reply");
    assert_eq!(received.load(Ordering::Relaxed), 1);
    let QueryResponse::Endpoint(endpoint) = a_handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("wrong response");
    };
    assert_eq!(endpoint.state, zero_api::EndpointRuntimeState::Running);
    assert!(endpoint.effective.outbound);
    assert!(endpoint.effective.inbound);
    let tcp_echo = TcpListener::bind((host, 0)).await.unwrap();
    let tcp_port = tcp_echo.local_addr().unwrap().port();
    let tcp_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = tcp_echo.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = [0; 256];
                while let Ok(size) = stream.read(&mut buffer).await {
                    if size == 0 || stream.write_all(&buffer[..size]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let mut stream = TcpStream::connect(("127.0.0.1", a_socks)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&host.octets());
    request.extend_from_slice(&tcp_port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    timeout(Duration::from_secs(15), stream.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response[1], 0, "SOCKS connection: {response:?}");
    stream.write_all(b"active-wireguard-flow").await.unwrap();
    let mut payload = [0; 21];
    timeout(Duration::from_secs(5), stream.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"active-wireguard-flow");
    let mut inbound = TcpStream::connect(("127.0.0.1", b_socks)).await.unwrap();
    inbound.write_all(&[5, 1, 0]).await.unwrap();
    inbound.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    inbound.write_all(&request).await.unwrap();
    inbound.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0, "inbound SOCKS connection: {response:?}");
    inbound.write_all(b"remote-inbound").await.unwrap();
    let mut inbound_payload = [0; 14];
    timeout(
        Duration::from_secs(5),
        inbound.read_exact(&mut inbound_payload),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&inbound_payload, b"remote-inbound");
    a_handle
        .execute_acknowledged(CommandRequest::EndpointSetDirections(
            EndpointSetDirectionsCommand {
                endpoint_id: "endpoint:wg".into(),
                directions: EndpointDirections::outbound_only(),
                persistence: EndpointPersistence::RuntimeOnly,
                expected_intent_revision: None,
            },
        ))
        .await
        .expect("live inbound contraction");
    let contracted = a_handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap();
    let QueryResponse::Endpoint(contracted) = contracted else {
        panic!("wrong endpoint response");
    };
    assert_eq!(contracted.state, zero_api::EndpointRuntimeState::Running);
    assert!(!contracted.effective.inbound && contracted.effective.outbound);
    assert!(UdpSocket::bind(("127.0.0.1", a_wire)).await.is_err());
    let closed = timeout(Duration::from_secs(2), inbound.read(&mut inbound_payload))
        .await
        .expect("existing inbound TCP closes");
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "inbound flow remained open: {closed:?}"
    );
    stream.write_all(b"outbound-still-live").await.unwrap();
    let mut after = [0; 19];
    timeout(Duration::from_secs(5), stream.read_exact(&mut after))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&after, b"outbound-still-live");
    assert!(timeout(
        Duration::from_secs(1),
        socks5_udp_echo_to(b_socks, target.clone(), echo_port, b"remote-new-business")
    )
    .await
    .is_err());
    assert_eq!(
        socks5_udp_echo_to(a_socks, target, echo_port, b"outbound-after-contract").await,
        b"outbound-after-contract"
    );
    assert_eq!(received.load(Ordering::Relaxed), 2);
    a_handle
        .execute_acknowledged(CommandRequest::EndpointSetState(EndpointSetStateCommand {
            endpoint_id: "endpoint:wg".into(),
            enabled: false,
            persistence: EndpointPersistence::RuntimeOnly,
            expected_intent_revision: None,
        }))
        .await
        .expect("stop confirms business termination");
    let closed = timeout(Duration::from_secs(2), stream.read(&mut payload))
        .await
        .expect("existing TCP closes");
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "flow remained open: {closed:?}"
    );
    assert!(UdpSocket::bind(("127.0.0.1", a_wire)).await.is_ok());
    let QueryResponse::Endpoint(other) = b_handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("wrong endpoint response")
    };
    assert_eq!(other.state, zero_api::EndpointRuntimeState::Running);
    assert!(UdpSocket::bind(("127.0.0.1", b_wire)).await.is_err());
    tcp_task.abort();
    echo_task.abort();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_endpoint_does_not_bind_its_physical_listener() {
    let port = free_udp_port();
    let occupied = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();
    let socks = free_port();
    let config = json!({"endpoints":[{"tag":"wg","enabled":false,
        "listen":{"address":"127.0.0.1","port":port}, "protocol":{
            "type":"wireguard", "private_key":STANDARD.encode([41;32]), "addresses":["10.0.0.1/32"],
            "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([42;32])).as_bytes()),
                "endpoint":"127.0.0.1:51820", "allowed_ips":["10.0.0.0/24"]}]}}],
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "route":{"rules":[],"final":{"type":"route","outbound":"wg"}}});
    let proxy = Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = spawn_engine(proxy);
    wait_for_listener(socks).await;
    let QueryResponse::Endpoint(endpoint) = handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("wrong response");
    };
    assert!(!endpoint.enabled);
    assert_eq!(endpoint.state, zero_api::EndpointRuntimeState::Stopped);
    assert_eq!(occupied.local_addr().unwrap().port(), port);
    running.shutdown().await.unwrap();
}

#[tokio::test]
async fn typed_endpoint_apply_acknowledges_the_materialized_candidate() {
    let socks = free_port();
    let initial = RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(initial.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = spawn_engine(proxy.clone());
    wait_for_listener(socks).await;
    let mut value = serde_json::to_value(initial).unwrap();
    value["endpoints"] = json!([{"tag":"wg","enabled":false,"protocol":{
        "type":"wireguard","private_key":STANDARD.encode([1;32]),"addresses":["10.0.0.1/32"],
        "peers":[{"public_key":STANDARD.encode([2;32]),"endpoint":"127.0.0.1:51820","allowed_ips":["10.0.0.0/24"]}]}}]);
    let candidate: RuntimeConfig = serde_json::from_value(value).unwrap();
    assert!(candidate.outbounds.is_empty());
    handle
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
        .await
        .expect("acknowledge canonical projections");
    assert_eq!(proxy.engine().config().outbounds.len(), 1);
    running.shutdown().await.unwrap();
}
