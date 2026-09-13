use base64::Engine;
use serde_json::json;
use zero_config::RuntimeConfig;

fn outbound(options: serde_json::Value) -> serde_json::Value {
    json!({
        "outbounds": [{
            "tag": "node",
            "protocol": {
                "type": "vless", "server": "proxy.test", "port": 443,
                "id": "00112233-4455-6677-8899-aabbccddeeff",
                "tls": {"server_name": "secret.example", "options": options}
            }
        }],
        "route": {"final": {"type": "route", "outbound": "node"}}
    })
}

fn ech_config_list() -> Vec<u8> {
    let name = b"public.example";
    let mut contents = vec![7, 0, 0x20, 0, 32];
    contents.extend_from_slice(&[9_u8; 32]);
    contents.extend_from_slice(&[0, 4, 0, 1, 0, 1, 0, name.len() as u8]);
    contents.extend_from_slice(name);
    contents.extend_from_slice(&[0, 0]);
    let mut config = vec![0xfe, 0x0d];
    config.extend_from_slice(&(contents.len() as u16).to_be_bytes());
    config.extend_from_slice(&contents);
    let mut list = Vec::new();
    list.extend_from_slice(&(config.len() as u16).to_be_bytes());
    list.extend_from_slice(&config);
    list
}

#[test]
fn parses_static_and_dns_ech_client_options() {
    let static_value = base64::engine::general_purpose::STANDARD.encode(ech_config_list());
    let parsed: RuntimeConfig = serde_json::from_value(outbound(json!({
        "ech_config_list": static_value,
        "ech_force_query": "half"
    })))
    .unwrap();
    parsed.validate().unwrap();

    let parsed: RuntimeConfig = serde_json::from_value(outbound(json!({
        "ech_config_list": "cover.example+https://1.1.1.1/dns-query",
        "ech_force_query": "none"
    })))
    .unwrap();
    parsed.validate().unwrap();

    let parsed: RuntimeConfig = serde_json::from_value(outbound(json!({
        "ech_config_list": "udp://1.1.1.1",
        "parameters": {
            "min_version": "1.0", "max_version": "1.3",
            "cipher_suites": ["TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"]
        }
    })))
    .unwrap();
    parsed.validate().unwrap();
}

#[test]
fn rejects_invalid_ech_source_and_tls_version() {
    for options in [
        json!({"ech_config_list": "%%%"}),
        json!({"ech_config_list": "one+two+udp://1.1.1.1"}),
        json!({"ech_config_list": "bad name+udp://1.1.1.1"}),
        json!({"ech_config_list": "tcp://1.1.1.1"}),
        json!({"ech_config_list": "udp://"}),
        json!({"ech_config_list": "udp://1.1.1.1/dns-query"}),
        json!({"backend": "open_ssl", "ech_config_list": "udp://1.1.1.1"}),
        json!({"ech_config_list": "udp://1.1.1.1", "parameters": {"max_version": "1.2"}}),
    ] {
        let parsed: RuntimeConfig = serde_json::from_value(outbound(options)).unwrap();
        assert!(parsed.validate().is_err());
    }

    let mut ip_name = outbound(json!({"ech_config_list": "udp://1.1.1.1"}));
    ip_name["outbounds"][0]["protocol"]["tls"]["server_name"] = json!("127.0.0.1");
    let parsed: RuntimeConfig = serde_json::from_value(ip_name).unwrap();
    assert!(parsed.validate().is_err());

    let mut download = outbound(json!({}));
    download["outbounds"][0]["protocol"]["split_http"] = json!({
        "mode": "packet-up",
        "download_settings": {
            "server": "127.0.0.1",
            "port": 443,
            "tls": {"options": {"ech_config_list": "udp://1.1.1.1"}},
            "split_http": {"mode": "packet-up"}
        }
    });
    let parsed: RuntimeConfig = serde_json::from_value(download).unwrap();
    assert!(parsed.validate().is_err());
}
