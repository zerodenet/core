use serde_json::{json, Value};
use zero_config::RuntimeConfig;
fn config() -> Value {
    json!({"inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":10001},"protocol":{"type":"vless","users":[{"id":"test"}]}}],"outbounds":[{"tag":"out","protocol":{"type":"vless","server":"127.0.0.1","port":10002,"id":"test"}}],"route":{"rules":[],"final":{"type":"route","outbound":"out"}}})
}
#[test]
fn xhttp_options_roundtrip_and_reject_invalid_shapes_before_listening() {
    let mut config = config();
    let options = json!({"mode":"packet-up","headers":{"X-Client":"zero"},"session_placement":"header","seq_placement":"query","uplink_http_method":"GET","uplink_data_placement":"cookie","uplink_data_key":"data","x_padding_obfs_mode":true,"x_padding_placement":"query","x_padding_method":"tokenish","x_padding_bytes":{"from":200,"to":300},"sc_max_each_post_bytes":1024,"sc_max_buffered_posts":9,"server_max_header_bytes":32768});
    for side in ["inbounds", "outbounds"] {
        config[side][0]["protocol"]["split_http"] = options.clone();
    }
    let parsed = RuntimeConfig::parse(&config.to_string()).unwrap();
    assert_eq!(
        RuntimeConfig::parse(&serde_json::to_string(&parsed).unwrap()).unwrap(),
        parsed
    );
    for (key, bad) in [
        ("seq_placement", json!("body")),
        ("x_padding_bytes", json!({"from":100,"to":99})),
        ("uplink_data_key", json!("bad;cookie")),
        ("headers", json!({"X-Client":"a\r\nb"})),
        ("headers", json!({"Content-Length":"1"})),
        ("sc_max_buffered_posts", json!(1000000)),
        ("unknown_option", json!(true)),
    ] {
        let mut bad_config = config.clone();
        bad_config["outbounds"][0]["protocol"]["split_http"][key] = bad;
        assert!(
            RuntimeConfig::parse(&bad_config.to_string()).is_err(),
            "accepted invalid {key}"
        );
    }
}
#[test]
fn fallback_rules_validate_destinations_and_legacy_conflicts() {
    let mut c = config();
    c["inbounds"][0]["protocol"]["fallback"] = json!({"rules":[{"path":"/app","destination":{"type":"unix","path":"backend.sock"},"proxy_protocol":2}]});
    RuntimeConfig::parse(&c.to_string()).unwrap();
    for (key, bad) in [
        ("path", json!("app")),
        ("proxy_protocol", json!(3)),
        (
            "destination",
            json!({"type":"tcp","server":"localhost","port":0}),
        ),
    ] {
        let mut b = c.clone();
        b["inbounds"][0]["protocol"]["fallback"]["rules"][0][key] = bad;
        assert!(RuntimeConfig::parse(&b.to_string()).is_err());
    }
    c["inbounds"][0]["protocol"]["fallback"]["server"] = json!("legacy");
    assert!(RuntimeConfig::parse(&c.to_string()).is_err());
}
#[test]
fn heartbeat_and_proxy_prelude_roundtrip_for_both_carriers() {
    for carrier in ["ws", "http_upgrade"] {
        let mut c = config();
        c["inbounds"][0]["protocol"][carrier] =
            json!({"path":"/ed?ed=2048","accept_proxy_protocol":true});
        if carrier == "ws" {
            c["inbounds"][0]["protocol"][carrier]["heartbeat_period_secs"] = json!(30);
        }
        let parsed = RuntimeConfig::parse(&c.to_string()).unwrap();
        assert_eq!(
            RuntimeConfig::parse(&serde_json::to_string(&parsed).unwrap()).unwrap(),
            parsed
        );
    }
}

#[test]
fn http3_uses_quic_tls_settings_and_keeps_stream_carriers_exclusive() {
    let mut c = config();
    c["inbounds"][0]["protocol"]["split_http"] = json!({"mode":"auto"});
    c["outbounds"][0]["protocol"]["split_http"] = json!({"mode":"stream-one"});
    c["inbounds"][0]["protocol"]["quic"] = json!({"cert_path":"cert.pem","key_path":"key.pem"});
    c["outbounds"][0]["protocol"]["quic"] =
        json!({"server_name":"localhost","ca_cert_path":"ca.pem"});
    RuntimeConfig::parse(&c.to_string()).unwrap();
    c["outbounds"][0]["protocol"]["tls"] = json!({"server_name":"localhost"});
    assert!(RuntimeConfig::parse(&c.to_string()).is_err());
    c["outbounds"][0]["protocol"]
        .as_object_mut()
        .unwrap()
        .remove("tls");
    c["inbounds"][0]["protocol"]["quic"]
        .as_object_mut()
        .unwrap()
        .remove("key_path");
    assert!(RuntimeConfig::parse(&c.to_string()).is_err());
}

#[test]
fn xhttp_download_and_xmux_validate_independent_native_carriers() {
    let mut c = config();
    c["outbounds"][0]["protocol"]["split_http"] = json!({"path":"/up/","xmux":{"max_connections":2},"download_settings":{"server":"download.test","port":443,"tls":{"server_name":"download.test"},"split_http":{"path":"/down/","xmux":{"max_concurrency":{"from":2,"to":4}}}}});
    let parsed = RuntimeConfig::parse(&c.to_string()).unwrap();
    assert_eq!(
        parsed,
        RuntimeConfig::parse(&serde_json::to_string(&parsed).unwrap()).unwrap()
    );
    for (key, value) in [
        ("mode", json!("stream-one")),
        ("xmux", json!({"max_connections":1,"max_concurrency":1})),
        ("xmux", json!({"h_max_reusable_secs":{"from":5,"to":1}})),
        ("download_settings", json!({"server":"x","port":0})),
        (
            "download_settings",
            json!({"server":"x","port":443,"tls":{},"quic":{}}),
        ),
        (
            "download_settings",
            json!({"server":"x","port":443,"reality":{"public_key":"invalid","short_id":""}}),
        ),
        (
            "download_settings",
            json!({"server":"x","port":443,"split_http":{"download_settings":{"server":"y","port":443}}}),
        ),
    ] {
        let mut bad = c.clone();
        bad["outbounds"][0]["protocol"]["split_http"][key] = value;
        assert!(
            RuntimeConfig::parse(&bad.to_string()).is_err(),
            "accepted {bad}"
        );
    }
    c["inbounds"][0]["protocol"]["split_http"] =
        c["outbounds"][0]["protocol"]["split_http"].clone();
    assert!(RuntimeConfig::parse(&c.to_string()).is_err());
}
