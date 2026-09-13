use serde_json::{json, Value};
use zero_config::RuntimeConfig;

fn config() -> Value {
    json!({
        "inbounds": [{
            "tag": "in",
            "listen": {"address": "127.0.0.1", "port": 12001},
            "protocol": {"type": "vless", "users": [{"id": "alice"}]}
        }],
        "outbounds": [{
            "tag": "out",
            "protocol": {
                "type": "vless",
                "server": "example.test",
                "port": 443,
                "id": "alice"
            }
        }],
        "route": {"rules": [], "final": {"type": "route", "outbound": "out"}}
    })
}

#[test]
fn browser_dialer_ws_and_packet_up_config_roundtrip() {
    for carrier in [
        json!({
            "ws": {"path": "/browser", "browser_dialer": {"listen": "127.0.0.1:16889"}},
            "tls": {"server_name": "EXAMPLE.TEST"}
        }),
        json!({
            "split_http": {
                "mode": "packet-up",
                "path": "/browser",
                "browser_dialer": {"listen": "[::1]:16890", "idle_capacity": 8}
            }
        }),
    ] {
        let mut value = config();
        value["outbounds"][0]["protocol"]
            .as_object_mut()
            .unwrap()
            .extend(carrier.as_object().unwrap().clone());
        let parsed = RuntimeConfig::parse(&value.to_string()).unwrap();
        let encoded = serde_json::to_string(&parsed).unwrap();
        assert_eq!(RuntimeConfig::parse(&encoded).unwrap(), parsed);
    }
}

#[test]
fn browser_dialer_rejects_unsupported_security_and_carriers() {
    for extra in [
        json!({"ws": {"browser_dialer": {}, "host": "front.example"}}),
        json!({"ws": {"browser_dialer": {}, "headers": {"X-Test": "one"}}}),
        json!({"ws": {"browser_dialer": {}}, "tls": {"insecure": true}}),
        json!({"ws": {"browser_dialer": {}}, "tls": {"server_name": "front.example"}}),
        json!({"ws": {"browser_dialer": {}}, "grpc": {"service_names": ["vless"]}}),
        json!({"ws": {"browser_dialer": {}}, "final_mask": {"tcp": [{
            "type": "fragment",
            "packets": {"minimum": 1, "maximum": 1},
            "length": {"minimum": 1, "maximum": 1}
        }]}}),
        json!({
            "ws": {"browser_dialer": {}},
            "split_http": {"mode": "packet-up", "browser_dialer": {}}
        }),
        json!({"split_http": {"mode": "stream-one", "browser_dialer": {}}}),
        json!({"split_http": {"mode": "packet-up", "browser_dialer": {}, "headers": {"X-Test": "one"}}}),
        json!({"split_http": {"mode": "packet-up", "browser_dialer": {}, "download_settings": {
            "server": "download.example", "port": 443
        }}}),
    ] {
        let mut value = config();
        value["outbounds"][0]["protocol"]
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(
            RuntimeConfig::parse(&value.to_string()).is_err(),
            "accepted unsupported Browser Dialer config: {value}"
        );
    }
}

#[test]
fn browser_dialer_rejects_public_listen_and_inbound_use() {
    let mut value = config();
    value["outbounds"][0]["protocol"]["ws"] =
        json!({"browser_dialer": {"listen": "0.0.0.0:16888"}});
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());

    let mut value = config();
    value["inbounds"][0]["protocol"]["ws"] = json!({"browser_dialer": {}});
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());
}

#[test]
fn browser_dialer_listener_claims_share_only_identical_settings() {
    let mut value = config();
    value["outbounds"][0]["protocol"]["ws"] = json!({"browser_dialer": {}});
    let mut second = value["outbounds"][0].clone();
    second["tag"] = json!("out-two");
    value["outbounds"].as_array_mut().unwrap().push(second);
    RuntimeConfig::parse(&value.to_string()).expect("identical listeners share one runtime");

    value["outbounds"][1]["protocol"]["ws"]["browser_dialer"]["idle_capacity"] = json!(32);
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());

    let mut value = config();
    value["outbounds"][0]["protocol"]["ws"] = json!({"browser_dialer": {}});
    value["inbounds"][0]["listen"] = json!({"address": "127.0.0.1", "port": 16888});
    assert!(RuntimeConfig::parse(&value.to_string()).is_err());
}
