#![cfg(all(feature = "socks5", feature = "vless"))]

mod support;
use base64::Engine;
use serde_json::{json, Value};
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};

const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
const ECH_CONFIG: &str =
    "ADn+DQA1agAgACBtuySC1pphjFlGYKTaSm2KWNg7GQVRS8uAYvLTm5QlGwAEAAEAAQAGZWcuY29tAAA=";

#[derive(Clone, Copy, Debug)]
enum Case {
    Ech(u16, u16),
    Legacy(&'static str),
}

#[tokio::test]
async fn native_ech_and_legacy_tls_carry_vless_tcp_and_udp() {
    for case in reference_cases() {
        run(case, None, false, false).await;
    }
}

#[tokio::test]
async fn ech_and_legacy_tls_match_pinned_official_in_both_directions() {
    let Some(binary) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    for case in reference_cases() {
        for official_server in [false, true] {
            run(case, Some(&binary), official_server, false).await;
        }
    }
}

#[tokio::test]
async fn real_ech_with_browser_fingerprint_matches_pinned_official() {
    let binary = support::interop::require_env("XRAY_BIN");
    // The pinned uTLS client accepts only HKDF-SHA256, with all three AEADs.
    for aead in 1..=3 {
        run(Case::Ech(1, aead), None, false, true).await;
        if let Some(binary) = &binary {
            for official_server in [false, true] {
                run(Case::Ech(1, aead), Some(binary), official_server, true).await;
            }
        }
    }
}

fn reference_cases() -> impl Iterator<Item = Case> {
    (1..=3)
        .flat_map(|kdf| (1..=3).map(move |aead| Case::Ech(kdf, aead)))
        .chain([Case::Legacy("1.0"), Case::Legacy("1.1")])
}

fn ech_config(case: Case) -> Vec<u8> {
    let mut config = base64::engine::general_purpose::STANDARD
        .decode(ECH_CONFIG)
        .unwrap();
    if let Case::Ech(kdf, aead) = case {
        config[45..47].copy_from_slice(&kdf.to_be_bytes());
        config[47..49].copy_from_slice(&aead.to_be_bytes());
    }
    config
}

fn server_keys(case: Case) -> String {
    let private = base64::engine::general_purpose::STANDARD
        .decode("MC4CAQAwBQYDK2VuBCIEIKBC3rocwIF5tGY+/TaYQrCxY+ULsch94ja9DojkcvlT")
        .unwrap();
    let config = ech_config(case);
    let mut keys = vec![0, 32];
    keys.extend_from_slice(&private[private.len() - 32..]);
    keys.extend_from_slice(&config);
    base64::engine::general_purpose::STANDARD.encode(keys)
}

async fn run(case: Case, binary: Option<&str>, official_server: bool, fingerprint: bool) {
    support::interop::init_logs("zero_transport=debug,vless=debug");
    eprintln!(
        "case={case:?} official={} server={official_server} fingerprint={fingerprint}",
        binary.is_some()
    );
    let material = TempMaterial::new("vless-ech-legacy");
    let tls = material.tls();
    let tunnel = free_port();
    let socks = free_port();
    let (version, cipher) = match case {
        Case::Ech(..) => ("1.3", None),
        Case::Legacy(version) => (version, Some("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA")),
    };
    let mut parameters = json!({"min_version":version,"max_version":version});
    let mut server_options = json!({});
    let mut client_options = json!({});
    let mut official_tls = json!({"minVersion":version,"maxVersion":version});
    if let Some(cipher) = cipher {
        parameters["cipher_suites"] = json!([cipher]);
        official_tls["cipherSuites"] = json!(cipher);
    }
    server_options["parameters"] = parameters.clone();
    client_options["parameters"] = parameters;
    let mut native_server_tls = json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
    let mut native_client_tls = json!({"server_name":"localhost","ca_cert_path":tls.cert_path});
    let mut xray_server_tls = official_tls.clone();
    xray_server_tls["certificates"] =
        json!([{"certificateFile":tls.cert_path,"keyFile":tls.key_path}]);
    let mut xray_client_tls = official_tls;
    xray_client_tls["serverName"] = json!("localhost");
    xray_client_tls["pinnedPeerCertSha256"] = json!(tls.cert_sha256_hex);
    // An empty fingerprint selects Chrome in this reference. Use its standard
    // Go TLS path for the full ECH KDF and legacy-version matrix; the separate
    // browser-fingerprint test exercises the reference's uTLS path.
    xray_client_tls["fingerprint"] = json!("unsafe");
    if matches!(case, Case::Ech(..)) {
        server_options["ech_server_keys"] = json!(server_keys(case));
        client_options["ech_config_list"] =
            json!(base64::engine::general_purpose::STANDARD.encode(ech_config(case)));
        xray_server_tls["echServerKeys"] = json!(server_keys(case));
        xray_client_tls["echConfigList"] =
            json!(base64::engine::general_purpose::STANDARD.encode(ech_config(case)));
    }
    if fingerprint {
        native_client_tls["client_fingerprint"] = json!("chrome");
        xray_client_tls["fingerprint"] = json!("chrome");
    }
    native_server_tls["options"] = server_options;
    native_client_tls["options"] = client_options;
    let native_server = json!({"inbounds":[{"tag":"vless","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{"type":"vless","users":[{"id":ID}],"tls":native_server_tls}}],"route":{"final":{"type":"direct"}}});
    let native_client = json!({"inbounds":[{"tag":"local","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],"outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,"tls":native_client_tls}}],"route":{"final":{"type":"route","outbound":"node"}}});
    let xray_server = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":tunnel,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":{"network":"raw","security":"tls","tlsSettings":xray_server_tls}}],"outbounds":[{"protocol":"freedom"}]});
    let xray_client = json!({"log":{"loglevel":"warning"},"inbounds":[{"listen":"127.0.0.1","port":socks,"protocol":"socks","settings":{"udp":true}}],"outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":tunnel,"id":ID,"encryption":"none"},"streamSettings":{"network":"raw","security":"tls","tlsSettings":xray_client_tls}}]});
    let mut engines = Vec::new();
    let mut processes = Vec::new();
    for (name, native, official, external) in [
        (
            "server",
            native_server,
            xray_server,
            binary.is_some() && official_server,
        ),
        (
            "client",
            native_client,
            xray_client,
            binary.is_some() && !official_server,
        ),
    ] {
        if external {
            let path = material.path(&format!("{name}.json"));
            std::fs::write(&path, official.to_string()).unwrap();
            processes.push(ExternalProcess::start(
                binary.unwrap().to_owned(),
                &["run", "-c", path.to_str().unwrap()],
                &material,
                name,
            ));
        } else {
            engines.push(start_native(native));
        }
    }
    let mut traffic = tokio::spawn(async move {
        wait_for_listener(tunnel).await;
        wait_for_listener(socks).await;
        let tcp_port = free_port();
        let payload = vec![0x5a; 32769];
        let echo = spawn_tcp_echo(tcp_port, payload.len()).await;
        assert_eq!(
            socks5_tcp_echo_once(socks, tcp_port, &payload)
                .await
                .unwrap(),
            payload
        );
        echo.await.unwrap();
        let udp_port = free_udp_port();
        let echo = spawn_udp_echo_count(udp_port, 3).await;
        let packets: &[&[u8]] = &[b"first", &[0x65; 1600], b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, udp_port, packets).await,
            packets
                .iter()
                .map(|packet| packet.to_vec())
                .collect::<Vec<_>>()
        );
        echo.await.unwrap();
    });
    let outcome = timeout(Duration::from_secs(20), &mut traffic).await;
    if !matches!(outcome, Ok(Ok(()))) {
        traffic.abort();
        panic!(
            "{case:?}: {outcome:?}; {}",
            processes
                .iter()
                .map(ExternalProcess::logs)
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
}

fn start_native(config: Value) -> zero_proxy::RunningProxy {
    spawn_engine(
        zero_proxy::Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap())
            .unwrap(),
    )
}
