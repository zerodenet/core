#![cfg(all(feature = "wireguard", feature = "socks5"))]

#[path = "support/host.rs"]
mod host;
mod support;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use serde_json::{json, Value};
use std::net::Ipv4Addr;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    task::JoinSet,
    time::{timeout, Duration},
};
use zero_api::{CommandRequest, CommandService, EndpointPersistence, EndpointSetStateCommand};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

fn endpoint(
    tag: &str,
    keys: (u8, u8),
    ports: (u16, u16),
    subnet: u8,
    host: Ipv4Addr,
    inbound: bool,
) -> Value {
    let (seed, peer) = keys;
    let (port, remote) = ports;
    json!({"tag":tag,"directions":{"inbound":inbound,"outbound":!inbound},
        "listen":{"address":"127.0.0.1","port":port},"protocol":{
            "type":"wireguard","private_key":STANDARD.encode([seed;32]),
            "addresses":[format!("10.0.{subnet}.{}/32",if inbound {2}else{1})],
            "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([peer;32])).as_bytes()),
                "endpoint":format!("127.0.0.1:{remote}"),
                "allowed_ips":[format!("10.0.{subnet}.0/24"),format!("{host}/32")],"keepalive_secs":1}]}})
}

fn proxy(value: Value) -> Proxy {
    Proxy::new(RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap()
}

async fn socks(port: u16, host: Ipv4Addr, target: u16) -> std::io::Result<TcpStream> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.write_all(&[5, 1, 0]).await?;
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await?;
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&host.octets());
    request.extend_from_slice(&target.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0 {
        return Err(std::io::Error::other(format!(
            "SOCKS rejected: {}",
            response[1]
        )));
    }
    Ok(stream)
}

async fn echo(stream: &mut TcpStream, payload: &[u8]) {
    stream.write_all(payload).await.unwrap();
    let mut returned = vec![0; payload.len()];
    timeout(Duration::from_secs(5), stream.read_exact(&mut returned))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(returned, payload);
}

#[tokio::test]
async fn stopping_a_ends_its_existing_business_while_b_keeps_its_tcp_flow() {
    let host = host::non_loopback_host_ipv4();
    let (a, ar, b, br) = (
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
        free_udp_port(),
    );
    let (sa, sb) = (free_port(), free_port());
    let local = proxy(json!({"endpoints":[
        endpoint("a",(81,82),(a,ar),1,host,false),endpoint("b",(83,84),(b,br),2,host,false)],
        "inbounds":[
            {"tag":"a-socks","listen":{"address":"127.0.0.1","port":sa},"protocol":{"type":"socks5"}},
            {"tag":"b-socks","listen":{"address":"127.0.0.1","port":sb},"protocol":{"type":"socks5"}}],
        "route":{"rules":[
            {"condition":{"type":"inbound","values":["a-socks"]},"action":{"type":"route","outbound":"a"}},
            {"condition":{"type":"inbound","values":["b-socks"]},"action":{"type":"route","outbound":"b"}}],
            "final":{"type":"direct"}}}));
    let handle = ProxyHandle::new(EngineHandle::new(local.engine().clone()), local.clone());
    let remote = |resource| {
        proxy(json!({"endpoints":[resource],"route":{"rules":[],"final":{"type":"direct"}}}))
    };
    let remote_a = spawn_engine(remote(endpoint(
        "remote-a",
        (82, 81),
        (ar, a),
        1,
        host,
        true,
    )));
    let remote_b = spawn_engine(remote(endpoint(
        "remote-b",
        (84, 83),
        (br, b),
        2,
        host,
        true,
    )));
    let running = spawn_engine(local);
    wait_for_listener(sa).await;
    wait_for_listener(sb).await;
    let listener = TcpListener::bind((host, 0)).await.unwrap();
    let target = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut clients = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut stream,_) = accepted.unwrap();
                    clients.spawn(async move {
                        let mut buffer = [0;1024];
                        while let Ok(size) = stream.read(&mut buffer).await {
                            if size == 0 { break; }
                            if stream.write_all(&buffer[..size]).await.is_err() { break; }
                        }
                    });
                },
                _ = clients.join_next(), if !clients.is_empty() => {},
            }
        }
    });
    let mut a_flow = timeout(Duration::from_secs(15), socks(sa, host, target))
        .await
        .unwrap()
        .unwrap();
    let mut b_flow = timeout(Duration::from_secs(15), socks(sb, host, target))
        .await
        .unwrap()
        .unwrap();
    echo(&mut a_flow, b"a-before-stop").await;
    echo(&mut b_flow, b"b-before-stop").await;
    handle
        .execute_acknowledged(CommandRequest::EndpointSetState(EndpointSetStateCommand {
            endpoint_id: "endpoint:a".into(),
            enabled: false,
            persistence: EndpointPersistence::RuntimeOnly,
            expected_intent_revision: None,
        }))
        .await
        .unwrap();
    let mut buffer = [0; 1];
    let closed = timeout(Duration::from_secs(2), a_flow.read(&mut buffer))
        .await
        .unwrap();
    assert!(
        matches!(closed, Ok(0) | Err(_)),
        "A flow remained open: {closed:?}"
    );
    assert!(
        timeout(Duration::from_secs(2), socks(sa, host, target))
            .await
            .unwrap()
            .is_err(),
        "disabled A must not fall through to Direct"
    );
    echo(&mut b_flow, b"b-after-a-stopped").await;
    assert!(UdpSocket::bind(("127.0.0.1", a)).await.is_ok());
    assert!(UdpSocket::bind(("127.0.0.1", b)).await.is_err());
    drop(b_flow);
    running.shutdown().await.unwrap();
    remote_a.shutdown().await.unwrap();
    remote_b.shutdown().await.unwrap();
    server.abort();
}
