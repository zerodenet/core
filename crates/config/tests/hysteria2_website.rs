use serde_json::{json, Value};
use zero_config::{ConfigError, RuntimeConfig};
fn parse(masquerade: Value, extra: Option<Value>) -> Result<RuntimeConfig, ConfigError> {
    let mut inbounds = vec![
        json!({"tag":"hy", "listen":{"address":"127.0.0.1","port":8443},
        "protocol":{"type":"hysteria2","password":"test","masquerade":masquerade}}),
    ];
    inbounds.extend(extra);
    RuntimeConfig::parse(
        &json!({"inbounds":inbounds,"route":{"rules":[],"final":{"type":"direct"}}}).to_string(),
    )
}
fn https() -> Value {
    json!({"address":"127.0.0.1","port":8443})
}
#[test]
fn website_preserves_existing_response_shapes_and_strict_fields() {
    for response in [
        json!({"type":"not_found"}),
        json!({"type":"file","dir":"site"}),
        json!({"type":"string","content":"hi"}),
        json!({"type":"proxy","url":"https://example.com/"}),
    ] {
        for extra in [
            json!({}),
            json!({"https":https(),"http":{"address":"127.0.0.1","port":8080},"force_https":true}),
        ] {
            let mut value = response.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let config = parse(value.clone(), None).unwrap();
            assert_eq!(
                RuntimeConfig::parse(&serde_json::to_string(&config).unwrap()).unwrap(),
                config
            );
            value["unknown"] = json!(true);
            assert!(parse(value, None).is_err());
        }
    }
    assert!(parse(
        json!({"type":"file","dir":"site","rewrite_host":true}),
        None
    )
    .is_err());
}
#[test]
fn website_validates_listeners_and_origin_options() {
    for value in [
        json!({"type":"not_found","http":https()}),
        json!({"type":"not_found","force_https":true}),
        json!({"type":"not_found","https":{"address":"","port":8443}}),
        json!({"type":"not_found","https":{"address":"127.0.0.1","port":0}}),
        json!({"type":"proxy","url":"ftp://localhost/"}),
        json!({"type":"proxy","url":"unix://remote/tmp/socket"}),
        json!({"type":"proxy","url":"unix:///tmp/socket?secret=1"}),
    ] {
        assert!(parse(value.clone(), None).is_err(), "{value}");
    }
    let value = json!({"type":"proxy","url":"https://localhost/","insecure":true,"rewrite_host":true,"x_forwarded":true,"https":https()});
    assert!(parse(value, None).is_ok());
    for url in ["/tmp/origin.sock", "unix:///tmp/origin.sock"] {
        assert_eq!(
            parse(json!({"type":"proxy","url":url}), None).is_ok(),
            cfg!(unix)
        );
    }
}
#[test]
fn website_tcp_claims_allow_quic_port_but_reject_other_tcp_listeners() {
    let site = json!({"type":"not_found","https":https()});
    assert!(
        parse(site.clone(), None).is_ok(),
        "TCP and UDP may use the same port"
    );
    for address in ["127.0.0.1", "0.0.0.0", "::", "::ffff:127.0.0.1"] {
        let extra = json!({"tag":"tcp","listen":{"address":address,"port":8443},"protocol":{"type":"direct"}});
        assert!(matches!(
            parse(site.clone(), Some(extra)),
            Err(ConfigError::DuplicateInboundListen { .. })
        ));
    }
    assert!(matches!(
        parse(
            json!({"type":"not_found","http":https(),"https":https()}),
            None
        ),
        Err(ConfigError::DuplicateInboundListen { .. })
    ));
    let extra = json!({"tag":"hy2","listen":{"address":"127.0.0.1","port":9443},"protocol":{"type":"hysteria2","password":"test","masquerade":site}});
    assert!(matches!(
        parse(site, Some(extra)),
        Err(ConfigError::DuplicateInboundListen { .. })
    ));
}
