#![cfg(all(feature = "vless", feature = "socks5"))]
mod support;
use serde_json::json;
use support::interop::{spawn_tcp_echo, TempMaterial};
use support::{free_port, spawn_engine, wait_for, wait_for_listener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_config::RuntimeConfig;
use zero_platform_tokio::TcpRelayStream;
use zero_proxy::Proxy;
use zero_transport::profile::OwnedClientTlsProfile;

#[tokio::test]
async fn ws_and_httpupgrade_proxy_preamble_precedes_tls_and_preserves_session_source() {
    for carrier in ["ws", "http_upgrade"] {
        for version in [1, 2] {
            let port = free_port();
            let echo_port = free_port();
            let material = TempMaterial::new("proxy-preamble");
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert_path = material.path("cert.pem");
            let key_path = material.path("key.pem");
            std::fs::write(&cert_path, cert.cert.pem()).unwrap();
            std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
            let mut config = json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"proxy-user"}],"tls":{"cert_path":cert_path,"key_path":key_path}}}],"route":{"rules":[],"final":{"type":"direct"}}});
            config["inbounds"][0]["protocol"][carrier] =
                json!({"path":"/tunnel","accept_proxy_protocol":true});
            let zero = spawn_engine(
                Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap(),
            );
            wait_for_listener(port).await;
            let echo = spawn_tcp_echo(echo_port, 4).await;
            let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap();
            let prefix = zero_transport::proxy_protocol::encode(
                version,
                Some("192.0.2.25:12345".parse().unwrap()),
                Some(format!("127.0.0.1:{port}").parse().unwrap()),
            )
            .unwrap();
            socket.write_all(&prefix).await.unwrap();
            let tls = OwnedClientTlsProfile {
                options: Default::default(),
                server_name: Some("localhost".into()),
                disable_sni: false,
                ca_cert_path: Some(cert_path.to_string_lossy().into_owned()),
                insecure: false,
                alpn: vec![],
                client_fingerprint: None,
            };
            let stream = zero_transport::tls::connect_tls_stream(socket, &tls, None, "localhost")
                .await
                .unwrap();
            let mut stream = if carrier == "ws" {
                let profile: zero_config::WebSocketConfig =
                    serde_json::from_value(json!({"path":"/tunnel"})).unwrap();
                TcpRelayStream::new(
                    zero_transport::ws::connect_ws(stream, &profile, "localhost", port)
                        .await
                        .unwrap(),
                )
            } else {
                let profile: zero_config::HttpUpgradeConfig =
                    serde_json::from_value(json!({"path":"/tunnel"})).unwrap();
                TcpRelayStream::new(
                    zero_transport::http_upgrade::connect_http_upgrade(stream, &profile)
                        .await
                        .unwrap(),
                )
            };
            let mut request = vec![0];
            request.extend(vless::parse_uuid("proxy-user").unwrap());
            request.extend([0, 1]);
            request.extend(echo_port.to_be_bytes());
            request.extend([1, 127, 0, 0, 1]);
            zero_traits::AsyncSocket::write_all(&mut stream, &request)
                .await
                .unwrap();
            let mut response = [0; 2];
            stream.read_exact(&mut response).await.unwrap();
            assert_eq!(response, [0, 0]);
            zero_traits::AsyncSocket::write_all(&mut stream, b"ping")
                .await
                .unwrap();
            let mut echoed = [0; 4];
            stream.read_exact(&mut echoed).await.unwrap();
            assert_eq!(&echoed, b"ping");
            drop(stream);
            echo.await.unwrap();
            wait_for("recorded proxy source", || {
                !zero.completed_sessions().is_empty()
            })
            .await;
            let completed = zero.completed_sessions();
            assert_eq!(
                completed[0].source_ip,
                Some(zero_core::Address::Ipv4([192, 0, 2, 25]))
            );
            assert_eq!(completed[0].source_port, Some(12345));
            zero.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn carrier_proxy_preamble_is_required_when_enabled_and_rejected_when_disabled() {
    for carrier in ["ws", "http_upgrade"] {
        for enabled in [true, false] {
            let port = free_port();
            let material = TempMaterial::new("proxy-preamble-rejection");
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let cert_path = material.path("cert.pem");
            let key_path = material.path("key.pem");
            std::fs::write(&cert_path, cert.cert.pem()).unwrap();
            std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
            let mut config = json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"proxy-user"}],"tls":{"cert_path":cert_path,"key_path":key_path}}}],"route":{"rules":[],"final":{"type":"direct"}}});
            config["inbounds"][0]["protocol"][carrier] =
                json!({"path":"/tunnel","accept_proxy_protocol":enabled});
            let zero = spawn_engine(
                Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap(),
            );
            wait_for_listener(port).await;
            for version in [1, 2] {
                let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .unwrap();
                if !enabled {
                    // A disabled listener must never interpret this supplied
                    // source address, including a valid version 2 preamble.
                    let prefix = zero_transport::proxy_protocol::encode(
                        version,
                        Some("192.0.2.25:12345".parse().unwrap()),
                        Some(format!("127.0.0.1:{port}").parse().unwrap()),
                    )
                    .unwrap();
                    socket.write_all(&prefix).await.unwrap();
                }
                let tls = OwnedClientTlsProfile {
                    options: Default::default(),
                    server_name: Some("localhost".into()),
                    disable_sni: false,
                    ca_cert_path: Some(cert_path.to_string_lossy().into_owned()),
                    insecure: false,
                    alpn: vec![],
                    client_fingerprint: None,
                };
                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    zero_transport::tls::connect_tls_stream(socket, &tls, None, "localhost"),
                )
                .await
                .expect("invalid carrier prelude is rejected before TLS completes");
                assert!(
                    result.is_err(),
                    "carrier={carrier} enabled={enabled} version={version}"
                );
            }
            assert!(zero.completed_sessions().is_empty());
            zero.shutdown().await.unwrap();
        }
    }
}
