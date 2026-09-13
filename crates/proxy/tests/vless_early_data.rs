#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::*;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

async fn interop(carrier: &str, zero_outbound: bool, secure: bool, early: bool) {
    let binary = std::env::var("XRAY_BIN").expect("official XRAY_BIN v26.3.27 required");
    let version = std::process::Command::new(&binary)
        .arg("version")
        .output()
        .unwrap();
    let version = String::from_utf8_lossy(&version.stdout);
    assert!(version.contains("26.3.27") && version.contains("d2758a0"));
    let material = TempMaterial::new("vless-early-data");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let port = free_port();
    let socks = free_port();
    let zero_field = if carrier == "ws" {
        "ws"
    } else {
        "http_upgrade"
    };
    let settings_field = if carrier == "ws" {
        "wsSettings"
    } else {
        "httpupgradeSettings"
    };
    let carrier_path = if early { "/early?ed=2048" } else { "/early" };
    let mut stream = json!({"network":carrier,"security":"none"});
    stream[settings_field] = json!({"host":"edge.test","path":carrier_path});
    if secure {
        stream["security"] = json!("tls");
        stream["tlsSettings"] = if zero_outbound {
            json!({"certificates":[{"certificateFile":cert_path,"keyFile":key_path}]})
        } else {
            json!({"serverName":"localhost","certificates":[{"certificateFile":cert_path,"usage":"verify"}]})
        };
    }
    let (mut zero, xray) = if zero_outbound {
        (
            json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"up","protocol":{"type":"vless","id":"early-user","server":"127.0.0.1","port":port,"xudp_concurrency":2}}],"route":{"rules":[],"final":{"type":"route","outbound":"up"}}}),
            json!({"log":{"loglevel":"debug"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"clients":[{"id":"early-user"}],"decryption":"none"},"streamSettings":stream}],"outbounds":[{"protocol":"freedom"}]}),
        )
    } else {
        (
            json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":"early-user"}]}}],"route":{"rules":[],"final":{"type":"direct"}}}),
            json!({"log":{"loglevel":"debug"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":port,"id":"early-user","encryption":"none"},"streamSettings":stream}]}),
        )
    };
    let protocol = if zero_outbound {
        &mut zero["outbounds"][0]["protocol"]
    } else {
        &mut zero["inbounds"][0]["protocol"]
    };
    protocol[zero_field] = json!({"host":"edge.test","path":carrier_path});
    if secure {
        protocol["tls"] = if zero_outbound {
            json!({"server_name":"localhost","ca_cert_path":cert_path})
        } else {
            json!({"cert_path":cert_path,"key_path":key_path})
        };
    }
    let zero = spawn_engine(Proxy::new(RuntimeConfig::parse(&zero.to_string()).unwrap()).unwrap());
    let path = material.path("xray.json");
    std::fs::write(&path, xray.to_string()).unwrap();
    let mut xray = XrayProcess::start(binary, &path, &material);
    wait_for_listener(port).await;
    wait_for_listener(socks).await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for size in [16, 32769] {
            let port = free_port();
            let payload = vec![159; size];
            let echo = spawn_tcp_echo(port, size).await;
            assert_eq!(
                socks5_tcp_echo_once(socks, port, &payload)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "{carrier}, zero_outbound={zero_outbound}, size={size}: {error}; {}",
                            xray.logs()
                        )
                    }),
                payload
            );
            echo.await.unwrap();
        }
        let port = free_udp_port();
        let echo = spawn_udp_echo_count(port, 2).await;
        assert_eq!(
            socks5_udp_echo_targets(socks, &[(port, b"first"), (port, b"second")]).await,
            vec![b"first".to_vec(), b"second".to_vec()]
        );
        echo.await.unwrap();
    })
    .await;
    xray.kill();
    zero.shutdown().await.unwrap();
    assert!(
        result.is_ok(),
        "{carrier}, zero_outbound={zero_outbound}: {}",
        xray.logs()
    );
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn websocket_early_data_official_both_directions() {
    interop("ws", true, false, true).await;
    interop("ws", false, false, true).await;
}
#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn httpupgrade_tls_early_data_official_both_directions() {
    interop("httpupgrade", true, true, true).await;
    interop("httpupgrade", false, true, true).await;
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn httpupgrade_plain_official_both_directions() {
    interop("httpupgrade", true, false, false).await;
    interop("httpupgrade", false, false, false).await;
}
