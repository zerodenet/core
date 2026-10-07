#![cfg(all(feature = "wireguard", feature = "socks5"))]

use crate::{host, support};

use std::{io, net::Ipv4Addr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{sleep, timeout, Duration},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;

use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

fn public_key(value: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([value; 32])).as_bytes())
}

async fn connect(socks_port: u16, target: Ipv4Addr, port: u16) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, socks_port)).await?;
    stream.write_all(&[5, 1, 0]).await?;
    let mut method = [0; 2];
    stream.read_exact(&mut method).await?;
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&target.octets());
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0 {
        return Err(io::Error::other(format!(
            "SOCKS CONNECT rejected: {}",
            response[1]
        )));
    }
    Ok(stream)
}

#[tokio::test]
async fn wireguard_tcp_udp_recover_after_network_changes_without_config_reload() {
    support::interop::init_logs("wireguard=debug");
    let server_port = free_udp_port();
    let socks_port = free_port();
    let host = host::non_loopback_host_ipv4();
    let server = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [{"tag":"wg-in", "listen":{"address":"127.0.0.1","port":server_port},
                "protocol":{"type":"wireguard", "private_key":STANDARD.encode([101;32]),
                    "peers":[{"public_key":public_key(102), "allowed_ips":["10.222.0.2/32"]}]}}],
            "outbounds":[{"tag":"direct", "protocol":{"type":"direct"}}],
            "route":{"rules":[], "final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let server = spawn_engine(Proxy::new(server).unwrap());
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[{"tag":"socks-in", "listen":{"address":"127.0.0.1","port":socks_port},
            "protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"wg", "protocol":{"type":"wireguard",
            "private_key":STANDARD.encode([102;32]), "addresses":["10.222.0.2/32"],
            "peers":[{"public_key":public_key(101), "endpoint":format!("127.0.0.1:{server_port}"),
                "allowed_ips":[format!("{host}/32")], "keepalive_secs":1}]}}],
        "route":{"rules":[], "final":{"type":"route", "outbound":"wg"}, "final_mode":"flow"}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let egress = proxy.egress_interface_control();
    let engine = proxy.engine().clone();
    let revision = engine.config_revision();
    let client = spawn_engine(proxy);
    wait_for_listener(socks_port).await;

    let tcp = TcpListener::bind((host, 0)).await.unwrap();
    let tcp_port = tcp.local_addr().unwrap().port();
    let tcp_echo = tokio::spawn(async move {
        loop {
            let (mut connection, _) = tcp.accept().await.unwrap();
            tokio::spawn(async move {
                let (mut reader, mut writer) = connection.split();
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
            });
        }
    });
    let udp = UdpSocket::bind((host, 0)).await.unwrap();
    let udp_port = udp.local_addr().unwrap().port();
    let udp_echo = tokio::spawn(async move {
        let mut buffer = [0; 2048];
        loop {
            let (size, peer) = udp.recv_from(&mut buffer).await.unwrap();
            udp.send_to(&buffer[..size], peer).await.unwrap();
        }
    });

    let mut previous_generation = None;
    for round in 0..3 {
        if round > 0 {
            // Command-driven TUN activation and route recovery change this
            // epoch without applying a new configuration.
            egress.invalidate_network();
        }
        let mut stream = timeout(Duration::from_secs(5), async {
            loop {
                match connect(socks_port, host, tcp_port).await {
                    Ok(stream) => break stream,
                    Err(_) => sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("WireGuard connection failed in network round {round}"));
        let payload = format!("network-recovery-round-{round}").into_bytes();
        stream.write_all(&payload).await.unwrap();
        let mut echoed = vec![0; payload.len()];
        timeout(Duration::from_secs(5), stream.read_exact(&mut echoed))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(echoed, payload);
        support::interop::socks5_udp_echo_to(
            socks_port,
            Address::Ipv4(host.octets()),
            udp_port,
            &payload,
        )
        .await;
        let query = zero_api::EndpointGetQuery {
            endpoint_id: "legacy:outbound:wg".into(),
        };
        let endpoint = timeout(Duration::from_secs(5), async {
            loop {
                let endpoint = engine.endpoint_snapshot(&query).unwrap();
                if endpoint.generation.is_some()
                    && (round == 0
                        || endpoint.recovery.as_ref().is_some_and(|r| {
                            r.phase == zero_api::EndpointRecoveryPhase::Recovered
                                && r.network_generation == egress.generation()
                        }))
                {
                    break endpoint;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if round > 0 {
            assert!(endpoint.generation > previous_generation);
        }
        previous_generation = endpoint.generation;
        assert_eq!(engine.config_revision(), revision);
    }
    tcp_echo.abort();
    udp_echo.abort();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}
