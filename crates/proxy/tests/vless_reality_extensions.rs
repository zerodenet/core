#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
const PRIVATE: &str = "OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04";
const PUBLIC: &str = "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA";
const SEED: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SHORT: &str = "0123456789abcdef";

#[tokio::test]
async fn mldsa65_authenticated_tcp_and_udp_match_pinned_official_both_directions() {
    run(None, false, "chrome").await;
    if let Some(binary) = support::interop::require_env("XRAY_BIN") {
        run(Some(binary.clone()), false, "chrome").await;
        run(Some(binary), true, "chrome").await;
    }
}

#[tokio::test]
async fn versioned_clienthello_profiles_relay_tcp_and_udp() {
    let official = support::interop::require_env("XRAY_BIN");
    for profile in [
        "chrome-83",
        "chrome-87",
        "chrome-96",
        "chrome-100",
        "chrome-102",
        "chrome-106",
        "chrome-120",
        "chrome-131",
        "chrome-133",
        "firefox-99",
        "firefox-102",
        "firefox-105",
        "firefox-120",
        "firefox-148",
        "ios-13",
        "ios-14",
        "edge-85",
        "edge-106",
        "safari-16.0",
        "safari-26.3",
        "360-11.0",
        "qq-11.1",
    ] {
        run(None, false, profile).await;
        if let Some(binary) = &official {
            run(Some(binary.clone()), true, profile).await;
        }
    }
}

async fn run(binary: Option<String>, official_server: bool, fingerprint: &str) {
    support::interop::init_logs("vless=debug,zero_transport=debug");
    eprintln!(
        "REALITY ML-DSA official={} official_server={official_server} fingerprint={fingerprint}",
        binary.is_some()
    );
    let tunnel = free_port();
    let socks = free_port();
    let target = free_port();
    let echo_port = free_port();
    let udp_port = free_udp_port();
    let material = TempMaterial::new("vless-reality-mldsa65");
    let decoy = spawn_decoy(target, binary.as_deref(), &material).await;
    let verify = vless::reality::mldsa::public_key(SEED).unwrap();
    let server = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{"type":"vless","users":[{"id":ID}],"reality":{"private_key":PRIVATE,"mldsa65_seed":SEED,"target":{"destination":{"type":"tcp","server":"127.0.0.1","port":target}},"short_ids":[SHORT],"server_names":["localhost"],"min_client_version":"26.3.27","max_client_version":"26.3.27","max_time_diff_ms":60_000}}}],"route":{"final":{"type":"direct"}}});
    let mut client = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,"reality":{"password":PUBLIC,"mldsa65_verify":verify,"server_name":"localhost","short_id":SHORT}}}],"route":{"final":{"type":"route","outbound":"node"}}});
    client["outbounds"][0]["protocol"]["reality"]["client_fingerprint"] = json!(fingerprint);
    let xray_server = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":tunnel,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":{"network":"raw","security":"reality","realitySettings":{"target":format!("127.0.0.1:{target}"),"serverNames":["localhost"],"privateKey":PRIVATE,"shortIds":[SHORT],"mldsa65Seed":SEED,"minClientVer":"26.3.27","maxClientVer":"26.3.27","maxTimeDiff":60_000}}}],"outbounds":[{"protocol":"freedom"}]});
    let xray_client = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":tunnel,"id":ID,"encryption":"none"},"streamSettings":{"network":"raw","security":"reality","realitySettings":{"serverName":"localhost","password":PUBLIC,"shortId":SHORT,"fingerprint":"chrome","mldsa65Verify":verify}}}]});
    let mut engines = Vec::new();
    let mut processes = Vec::new();
    for (name, native, official, external) in [
        (
            "server",
            server,
            xray_server,
            binary.is_some() && official_server,
        ),
        (
            "client",
            client,
            xray_client,
            binary.is_some() && !official_server,
        ),
    ] {
        if external {
            let path = material.path(&format!("{name}.json"));
            std::fs::write(&path, official.to_string()).unwrap();
            processes.push(ExternalProcess::start(
                binary.clone().unwrap(),
                &["run", "-c", path.to_str().unwrap()],
                &material,
                name,
            ));
        } else {
            engines.push(spawn_engine(
                zero_proxy::Proxy::new(
                    zero_config::RuntimeConfig::parse(&native.to_string()).unwrap(),
                )
                .unwrap(),
            ));
        }
    }
    wait_for_listener(tunnel).await;
    wait_for_listener(socks).await;
    timeout(Duration::from_secs(20), async {
        let payload = vec![0x36; 65537];
        let echo = spawn_tcp_echo(echo_port, payload.len()).await;
        assert_eq!(
            socks5_tcp_echo_once(socks, echo_port, &payload)
                .await
                .unwrap(),
            payload
        );
        echo.await.unwrap();
        let echo = spawn_udp_echo_count(udp_port, 3).await;
        let large = vec![0x51; 1600];
        let packets: &[&[u8]] = &[b"first", &large, b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, udp_port, packets).await,
            packets.iter().map(|b| b.to_vec()).collect::<Vec<_>>()
        );
        echo.await.unwrap();
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "ML-DSA interop failed official_server={official_server}: {error}; {}",
            processes
                .iter()
                .map(ExternalProcess::logs)
                .collect::<Vec<_>>()
                .join("\n")
        )
    });
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
    drop(decoy);
}

enum Decoy {
    Native(tokio::task::JoinHandle<()>),
    Official(ExternalProcess),
}
impl Drop for Decoy {
    fn drop(&mut self) {
        match self {
            Self::Native(task) => task.abort(),
            Self::Official(process) => process.kill(),
        }
    }
}
async fn spawn_decoy(port: u16, binary: Option<&str>, material: &TempMaterial) -> Decoy {
    if let Some(binary) = binary {
        let tls = material.tls();
        let chain = material.path("target-chain.pem");
        std::fs::write(
            &chain,
            std::fs::read_to_string(&tls.cert_path).unwrap().repeat(16),
        )
        .unwrap();
        let path = material.path("target.json");
        let config = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":port,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":{"network":"raw","security":"tls","tlsSettings":{"alpn":["h2","http/1.1"],"minVersion":"1.3","curvePreferences":["X25519MLKEM768","X25519"],"certificates":[{"certificateFile":chain,"keyFile":tls.key_path}]}}}],"outbounds":[{"protocol":"freedom"}]});
        std::fs::write(&path, config.to_string()).unwrap();
        let process = ExternalProcess::start(
            binary.into(),
            &["run", "-c", path.to_str().unwrap()],
            material,
            "target",
        );
        wait_for_listener(port).await;
        return Decoy::Official(process);
    }
    use std::sync::Arc;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into();
    let mut config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone(); 16], key)
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    Decoy::Native(tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((socket,_)) = accepted else { break };
                    let acceptor = acceptor.clone();
                    tasks.spawn(async move {
                        if let Ok(Ok(mut stream)) = timeout(Duration::from_secs(15),acceptor.accept(socket)).await {
                            let _ = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await;
                        }
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    }))
}

#[tokio::test]
async fn unauthenticated_tls_and_plain_connections_are_forwarded_to_the_configured_target() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let tunnel = free_port();
    let target = free_port();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", target))
        .await
        .unwrap();
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = seen.clone();
    let target_task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket,_) = accepted.unwrap();
                    let observed = observed.clone();
                    tasks.spawn(async move {
                        // An ordinary target answers arbitrary unauthenticated input.
                        let mut header = Vec::new();
                        loop {
                            let mut byte = [0];
                            if socket.read_exact(&mut byte).await.is_err() {return;}
                            header.push(byte[0]);
                            if header.ends_with(b"\r\n") {break;}
                        }
                        assert!(header.starts_with(b"PROXY TCP4 127.0.0.1 127.0.0.1 "));
                        let mut payload = Vec::new();
                        socket.read_to_end(&mut payload).await.unwrap();
                        if !payload.is_empty() {
                            observed.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
                            socket.write_all(&payload).await.unwrap();
                        }
                        socket.shutdown().await.unwrap();
                    });
                }
                finished = tasks.join_next(), if !tasks.is_empty() => {finished.unwrap().unwrap();}
            }
        }
    });
    let config = json!({"inbounds":[{"tag":"reality","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{"type":"vless","users":[{"id":ID}],"reality":{"private_key":PRIVATE,"short_ids":[SHORT],"server_names":["localhost"],"target":{"destination":{"type":"tcp","server":"127.0.0.1","port":target},"proxy_protocol":1}}}}],"route":{"final":{"type":"direct"}}});
    let engine = spawn_engine(
        zero_proxy::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap())
            .unwrap(),
    );
    wait_for_listener(tunnel).await;
    let mismatched = {
        let (_, public) = vless::reality::generate_reality_key_pair();
        let public = vless::reality::reality_util::decode_public_key(&public).unwrap();
        let mut client = vless::reality::reality_client_connection::RealityClientConnection::new(
            vless::reality::reality_client_connection::RealityClientConfig {
                public_key: public,
                server_name: "localhost".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let mut bytes = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut bytes).unwrap();
        }
        bytes
    };
    for payload in [
        b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".to_vec(),
        mismatched,
    ] {
        timeout(Duration::from_secs(5), async {
            let mut client = tokio::net::TcpStream::connect(("127.0.0.1", tunnel))
                .await
                .unwrap();
            client.write_all(&payload).await.unwrap();
            client.shutdown().await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, payload);
        })
        .await
        .unwrap();
    }
    assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 2);
    engine.shutdown().await.unwrap();
    target_task.abort();
}
