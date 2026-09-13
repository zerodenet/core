use super::interop::*;
use super::{free_port, spawn_engine, wait_for_listener};
use serde_json::json;
use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
const ID: &str = "11111111-2222-3333-4444-555555555555";
pub async fn interop_transport(mode: &str, zero_client: bool, tls: bool) {
    interop_options(mode, zero_client, tls, json!({}), json!({})).await;
}
pub async fn interop_options(
    mode: &str,
    zero_client: bool,
    tls: bool,
    native: serde_json::Value,
    official: serde_json::Value,
) {
    eprintln!("XHTTP reference case mode={mode} zero_client={zero_client} native={native}");
    let h3 = official
        .get("_test_http3")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut official = official;
    official.as_object_mut().unwrap().remove("_test_http3");
    let relay = official
        .as_object_mut()
        .unwrap()
        .remove("_test_relay")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        !relay || (zero_client && !h3),
        "relay fixture requires a Zero stream-carrier client"
    );
    let separate_download = official
        .as_object_mut()
        .unwrap()
        .remove("_test_download")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let server_name = official
        .as_object_mut()
        .unwrap()
        .remove("_test_server_name")
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "localhost".into());
    init_logs("zero_proxy=warn,vless=warn");
    let binary = std::env::var("XRAY_BIN").expect("XRAY_BIN must point to official v26.3.27");
    let version = std::process::Command::new(&binary)
        .arg("version")
        .output()
        .unwrap();
    let version = String::from_utf8_lossy(&version.stdout);
    assert!(
        version.contains("26.3.27") && version.contains("d2758a0"),
        "wrong reference: {version}"
    );
    let port = free_port();
    let socks = free_port();
    let material = TempMaterial::new("xhttp-modes-official");
    let settings = json!({"network":"xhttp", "security":"none", "xhttpSettings":{"path":"/tunnel/", "mode":mode}});
    let (mut zero_config, mut xray_config) = if zero_client {
        (
            json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"upstream","protocol":{"type":"vless","server":"127.0.0.1","port":port,"id":ID,"split_http":{"path":"/tunnel/","mode":mode}}}],
            "route":{"rules":[],"final":{"type":"route","outbound":"upstream"}}}),
            json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"clients":[{"id":ID}],"decryption":"none"},"streamSettings":settings}],"outbounds":[{"protocol":"freedom"}]}),
        )
    } else {
        (
            json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"vless","users":[{"id":ID}],"split_http":{"path":"/tunnel/","mode":"auto"}}}],
            "outbounds":[],"route":{"rules":[],"final":{"type":"direct"}}}),
            json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"auth":"noauth","udp":true}}],
            "outbounds":[{"protocol":"vless","settings":{"vnext":[{"address":"127.0.0.1","port":port,"users":[{"id":ID,"encryption":"none"}]}]},"streamSettings":settings}]}),
        )
    };
    let (zero_side, xray_side) = if zero_client {
        ("outbounds", "inbounds")
    } else {
        ("inbounds", "outbounds")
    };
    zero_config[zero_side][0]["protocol"]["split_http"]
        .as_object_mut()
        .unwrap()
        .extend(native.as_object().unwrap().clone());
    xray_config[xray_side][0]["streamSettings"]["xhttpSettings"]
        .as_object_mut()
        .unwrap()
        .extend(official.as_object().unwrap().clone());
    if tls {
        let certificate = rcgen::generate_simple_self_signed(vec![server_name.clone()]).unwrap();
        let fingerprint: String =
            ring::digest::digest(&ring::digest::SHA256, certificate.cert.der().as_ref())
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        std::fs::write(&cert_path, certificate.cert.pem()).unwrap();
        std::fs::write(&key_path, certificate.signing_key.serialize_pem()).unwrap();
        if zero_client {
            zero_config["outbounds"][0]["protocol"]["tls"] =
                json!({"server_name":server_name, "insecure":true});
            xray_config["inbounds"][0]["streamSettings"]["security"] = json!("tls");
            xray_config["inbounds"][0]["streamSettings"]["tlsSettings"] = json!({"alpn":["h2","http/1.1"],"certificates":[{"certificateFile":cert_path,"keyFile":key_path}]});
        } else {
            zero_config["inbounds"][0]["protocol"]["tls"] =
                json!({"cert_path":cert_path,"key_path":key_path,"alpn":["h2","http/1.1"]});
            xray_config["outbounds"][0]["streamSettings"]["security"] = json!("tls");
            xray_config["outbounds"][0]["streamSettings"]["tlsSettings"] =
                json!({"serverName":server_name,"pinnedPeerCertSha256":fingerprint,"alpn":["h2"]});
        }
    }
    if h3 {
        let (zero_side, xray_side) = if zero_client {
            ("outbounds", "inbounds")
        } else {
            ("inbounds", "outbounds")
        };
        let mut zero_tls = zero_config[zero_side][0]["protocol"]
            .as_object_mut()
            .unwrap()
            .remove("tls")
            .unwrap();
        zero_tls.as_object_mut().unwrap().remove("alpn");
        zero_config[zero_side][0]["protocol"]["quic"] = zero_tls;
        xray_config[xray_side][0]["streamSettings"]["tlsSettings"]["alpn"] = json!(["h3"]);
    }
    let mut download_forwarder = None;
    let download_connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    if separate_download {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let download_port = listener.local_addr().unwrap().port();
        let counter = download_connections.clone();
        download_forwarder = Some(tokio::spawn(async move {
            let mut jobs = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (mut incoming, _) = accepted.unwrap(); counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        jobs.spawn(async move { let mut outgoing = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap(); let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await; });
                    },
                    _ = jobs.join_next(), if !jobs.is_empty() => {},
                }
            }
        }));
        if zero_client {
            let protocol = &mut zero_config["outbounds"][0]["protocol"];
            protocol["split_http"]["download_settings"] = json!({"server":"127.0.0.1","port":download_port,"tls":protocol.get("tls").cloned(),"split_http":{"path":"/tunnel/","xmux":{"max_connections":1}}});
        } else {
            let settings = &mut xray_config["outbounds"][0]["streamSettings"];
            let mut download = settings.clone();
            download["address"] = json!("127.0.0.1");
            download["port"] = json!(download_port);
            download["xhttpSettings"]["xmux"] = json!({"maxConnections":1});
            settings["xhttpSettings"]["downloadSettings"] = download;
        }
    }
    let tap = if !tls && !h3 {
        super::xhttp_tap::start(port).await
    } else {
        None
    };
    if let Some((tap_port, _)) = &tap {
        if zero_client {
            zero_config["outbounds"][0]["protocol"]["port"] = json!(*tap_port);
        } else {
            xray_config["outbounds"][0]["settings"]["vnext"][0]["port"] = json!(*tap_port);
        }
    }
    let relay = if relay {
        let relay_port = free_port();
        let first = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
            "inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":relay_port},"protocol":{"type":"socks5"}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        }).to_string()).unwrap()).unwrap());
        wait_for_listener(relay_port).await;
        zero_config["outbounds"].as_array_mut().unwrap().push(json!({"tag":"first","protocol":{"type":"socks5","server":"127.0.0.1","port":relay_port}}));
        zero_config["outbound_groups"] =
            json!([{"tag":"relay","type":"relay","proxies":["first","upstream"]}]);
        zero_config["route"]["final"] = json!({"type":"route","outbound":"relay"});
        Some(first)
    } else {
        None
    };
    let zero =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&zero_config.to_string()).unwrap()).unwrap());
    let path = material.path("xray.json");
    std::fs::write(&path, xray_config.to_string()).unwrap();
    let validation = std::process::Command::new(&binary)
        .args(["run", "-test", "-config"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        validation.status.success(),
        "{} {}",
        String::from_utf8_lossy(&validation.stdout),
        String::from_utf8_lossy(&validation.stderr)
    );
    let mut xray = XrayProcess::start(binary, &path, &material);
    if !h3 {
        wait_for_listener(port).await;
    }
    wait_for_listener(socks).await;
    let result = timeout(Duration::from_secs(40), async {
        let mut jobs = tokio::task::JoinSet::new();
        for byte in 0..4u8 {
            let echo = free_port();
            let payload = vec![byte; 131_073];
            let server = spawn_tcp_echo(echo, payload.len()).await;
            jobs.spawn(async move {
                let received = socks5_tcp_echo_once(socks, echo, &payload).await?;
                assert_eq!(received, payload);
                server.await.map_err(std::io::Error::other)?;
                Ok::<_, std::io::Error>(())
            });
        }
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp = socket.local_addr().unwrap().port();
        let echo = tokio::spawn(async move {
            let mut buffer = vec![0; 1601];
            let (size, peer) = socket.recv_from(&mut buffer).await.unwrap();
            assert_eq!(size, 1600);
            socket.send_to(&buffer[..size], peer).await.unwrap();
        });
        let payload = vec![9; 1600];
        assert_eq!(socks5_udp_echo(socks, udp, &payload).await, payload);
        echo.await.unwrap();
        while let Some(job) = jobs.join_next().await {
            job.map_err(std::io::Error::other)??;
        }
        Ok::<_, std::io::Error>(())
    })
    .await;
    if let Some(forwarder) = download_forwarder {
        forwarder.abort();
        assert!(download_connections.load(std::sync::atomic::Ordering::SeqCst) > 0);
    }
    drop(tap);
    xray.kill();
    zero.shutdown().await.unwrap();
    if let Some(relay) = relay {
        relay.shutdown().await.unwrap();
    }
    assert!(
        matches!(result, Ok(Ok(()))),
        "mode={mode} zero_client={zero_client} result={result:?}: {}",
        xray.logs()
    );
}
