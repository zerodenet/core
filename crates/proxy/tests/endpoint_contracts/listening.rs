#![cfg(all(feature = "wireguard", feature = "socks5"))]

use super::{host, support};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::json;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{timeout, Duration},
};
use zero_api::{
    CommandRequest, CommandService, EndpointDirections, EndpointGetQuery, EndpointPersistence,
    EndpointSetDirectionsCommand, QueryRequest, QueryResponse, QueryService,
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

async fn connect(socks: u16, host: std::net::Ipv4Addr, port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", socks)).await.unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1];
    request.extend(host.octets());
    request.extend(port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0, "SOCKS response {response:?}");
    stream
}

async fn echo(stream: &mut TcpStream, payload: &[u8]) {
    stream.write_all(payload).await.unwrap();
    let mut reply = vec![0; payload.len()];
    timeout(Duration::from_secs(5), stream.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, payload);
}

fn endpoint(handle: &ProxyHandle) -> zero_api::EndpointSnapshot {
    let QueryResponse::Endpoint(endpoint) = handle
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("endpoint response");
    };
    endpoint
}

#[tokio::test]
async fn passive_peer_learns_authenticated_address_and_live_outbound_revocation_preserves_inbound()
{
    let host = host::non_loopback_host_ipv4();
    let a_wire = free_udp_port();
    let b_wire = free_udp_port();
    let a_socks = free_port();
    let b_socks = free_port();
    let config = |private: u8, remote_private: u8, listen, remote: Option<u16>, socks, address| {
        let mut peer = json!({
            "public_key": STANDARD.encode(PublicKey::from(&StaticSecret::from([remote_private;32])).as_bytes()),
            "allowed_ips": ["10.0.0.0/24", format!("{host}/32")], "keepalive_secs":1
        });
        if let Some(remote) = remote {
            peer["endpoint"] = json!(format!("127.0.0.1:{remote}"));
        }
        RuntimeConfig::parse(&json!({
            "endpoints":[{"tag":"wg", "directions":{"inbound":true,"outbound":true},
                "listen":{"address":"127.0.0.1","port":listen}, "protocol":{
                    "type":"wireguard", "private_key":STANDARD.encode([private;32]),
                    "addresses":[address], "peers":[peer]}}],
            "inbounds":[{"tag":"socks", "listen":{"address":"127.0.0.1","port":socks}, "protocol":{"type":"socks5"}}],
            "route":{"rules":[{"condition":{"type":"inbound","values":["socks"]},
                "action":{"type":"route","outbound":"wg"}}], "final":{"type":"direct"}}
        }).to_string()).unwrap()
    };
    let a_config = config(71, 72, a_wire, None, a_socks, "10.0.0.1/32");
    let a_proxy = Proxy::new(a_config.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(a_proxy.engine().clone()), a_proxy.clone());
    let a = spawn_engine(a_proxy);
    let b = spawn_engine(
        Proxy::new(config(72, 71, b_wire, Some(a_wire), b_socks, "10.0.0.2/32")).unwrap(),
    );
    wait_for_listener(a_socks).await;
    wait_for_listener(b_socks).await;

    let udp = UdpSocket::bind((host, 0)).await.unwrap();
    let port = udp.local_addr().unwrap().port();
    let udp_task = tokio::spawn(async move {
        let mut buffer = [0; 1024];
        loop {
            let (size, source) = udp.recv_from(&mut buffer).await.unwrap();
            udp.send_to(&buffer[..size], source).await.unwrap();
        }
    });
    let reply = timeout(
        Duration::from_secs(20),
        support::interop::socks5_udp_echo_to(
            b_socks,
            Address::Ipv4(host.octets()),
            port,
            b"learn-listening-peer",
        ),
    )
    .await
    .expect("passive peer handshake");
    assert_eq!(reply, b"learn-listening-peer");
    let QueryResponse::EndpointDetails(details) = handle
        .query(QueryRequest::EndpointDetails(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("details response");
    };
    assert!(details.details["peers"][0]["configured_endpoint"].is_null());
    assert_eq!(details.details["peers"][0]["source_known"], true);
    assert_eq!(
        details.details["peers"][0]["authenticated_endpoint"],
        format!("127.0.0.1:{b_wire}")
    );

    let tcp = TcpListener::bind((host, 0)).await.unwrap();
    let tcp_port = tcp.local_addr().unwrap().port();
    let tcp_task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = tcp.accept().await.unwrap();
            tokio::spawn(async move {
                let mut buffer = [0; 1024];
                while let Ok(size) = stream.read(&mut buffer).await {
                    if size == 0 || stream.write_all(&buffer[..size]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    let mut outbound = timeout(Duration::from_secs(20), connect(a_socks, host, tcp_port))
        .await
        .unwrap();
    let mut inbound = timeout(Duration::from_secs(20), connect(b_socks, host, tcp_port))
        .await
        .unwrap();
    echo(&mut outbound, b"outbound-before").await;
    echo(&mut inbound, b"inbound-before").await;
    // A rejected candidate must not revoke the existing stacks or sessions.
    let occupied = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut rejected = a_config;
    rejected.endpoints[0].directions.outbound = false;
    rejected.inbounds.push(serde_json::from_value(json!({"tag":"occupied", "listen":{"address":"127.0.0.1", "port":occupied.local_addr().unwrap().port()}, "protocol":{"type":"socks5"}})).unwrap());
    assert!(handle
        .apply_runtime_config_and_wait(rejected, Duration::from_secs(5))
        .await
        .is_err());
    assert!(endpoint(&handle).effective.outbound);
    echo(&mut outbound, b"rollback-outbound-live").await;
    echo(&mut inbound, b"rollback-inbound-live").await;
    drop(occupied);
    let before = endpoint(&handle);
    let directions = |outbound| {
        CommandRequest::EndpointSetDirections(EndpointSetDirectionsCommand {
            endpoint_id: "endpoint:wg".into(),
            directions: EndpointDirections {
                inbound: true,
                outbound,
            },
            persistence: EndpointPersistence::RuntimeOnly,
            expected_intent_revision: None,
            expected_core_instance_id: None,
        })
    };
    handle
        .execute_acknowledged(directions(false))
        .await
        .unwrap();
    let contracted = endpoint(&handle);
    assert!(contracted.effective.inbound && !contracted.effective.outbound);
    assert_eq!(contracted.generation, before.generation);
    assert_eq!(contracted.started_at_unix_ms, before.started_at_unix_ms);
    assert!(
        UdpSocket::bind(("127.0.0.1", a_wire)).await.is_err(),
        "listener remains bound"
    );
    let mut buffer = [0; 32];
    let closed = timeout(Duration::from_secs(2), outbound.read(&mut buffer))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "revoked connection remained open {closed:?}"
    );
    echo(&mut inbound, b"inbound-remains-live").await;
    handle.execute_acknowledged(directions(true)).await.unwrap();
    let mut resumed = timeout(Duration::from_secs(20), connect(a_socks, host, tcp_port))
        .await
        .unwrap();
    echo(&mut resumed, b"learned-address-survives").await;
    echo(&mut inbound, b"inbound-after-resume").await;
    assert_eq!(endpoint(&handle).generation, before.generation);
    drop(resumed);
    drop(inbound);
    tcp_task.abort();
    udp_task.abort();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
