use serde_json::json;
use zero_config::RuntimeConfig;
#[test]
fn reverse_requires_unique_user_portal_and_virtual_ingress_references() {
    let mut config = json!({
        "inbounds":[{"tag":"in","listen":{"address":"127.0.0.1","port":11001},"protocol":{"type":"vless","users":[{"id":"alice","reverse_tag":"portal"}]}}],
        "outbounds":[{"tag":"portal","protocol":{"type":"vless_reverse"}}, {"tag":"link","protocol":{"type":"vless","server":"localhost","port":11002,"id":"alice","reverse_tag":"bridge"}}],
        "route":{"rules":[{"condition":{"type":"inbound","values":["bridge"]},"action":{"type":"direct"}}],"final":{"type":"route","outbound":"portal"}}
    });
    let parsed = RuntimeConfig::parse(&config.to_string()).unwrap();
    assert!(parsed.outbounds[0].protocol.endpoint().is_none());
    config["inbounds"][0]["protocol"]["users"][0]["reverse_tag"] = json!("link");
    assert!(RuntimeConfig::parse(&config.to_string())
        .unwrap_err()
        .to_string()
        .contains("vless_reverse"));
    config["inbounds"][0]["protocol"]["users"][0]["reverse_tag"] = json!("portal");
    config["outbounds"][1]["protocol"]["reverse_tag"] = json!("in");
    assert!(RuntimeConfig::parse(&config.to_string()).is_err());
}

#[test]
fn reverse_sniffing_accepts_xray_names_and_rejects_inert_or_unknown_policies() {
    let mut config = json!({
        "inbounds":[],
        "outbounds":[{"tag":"link","protocol":{
            "type":"vless","server":"localhost","port":11002,"id":"alice",
            "reverse_tag":"bridge",
            "reverse_sniffing":{
                "enabled":true,
                "destOverride":["http","tls","quic","fakedns"],
                "domainsExcluded":["blocked.example","regexp:^private\\."],
                "metadataOnly":false,
                "routeOnly":true
            }
        }}],
        "route":{"rules":[{"condition":{"type":"inbound","values":["bridge"]},"action":{"type":"direct"}}],"final":{"type":"direct"}}
    });
    let parsed = RuntimeConfig::parse(&config.to_string()).unwrap();
    let zero_config::OutboundProtocolConfig::Vless {
        reverse_sniffing: Some(sniffing),
        ..
    } = &parsed.outbounds[0].protocol
    else {
        panic!("expected VLESS reverse sniffing");
    };
    assert!(sniffing.enabled);
    assert!(sniffing.route_only);
    assert_eq!(
        sniffing.destination_override,
        ["http", "tls", "quic", "fakedns"]
    );

    config["outbounds"][0]["protocol"]["reverse_tag"] = serde_json::Value::Null;
    assert!(RuntimeConfig::parse(&config.to_string())
        .unwrap_err()
        .to_string()
        .contains("requires reverse_tag"));

    config["outbounds"][0]["protocol"]["reverse_tag"] = json!("bridge");
    config["outbounds"][0]["protocol"]["reverse_sniffing"]["destOverride"] = json!(["bittorrent"]);
    assert!(RuntimeConfig::parse(&config.to_string())
        .unwrap_err()
        .to_string()
        .contains("unknown destination override"));
}
