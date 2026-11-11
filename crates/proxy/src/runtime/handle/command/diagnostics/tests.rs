use super::*;

#[cfg(feature = "hysteria2")]
#[tokio::test]
async fn standard_outbound_probe_endpoint_needs_no_compatibility_executor() {
    // HY2 has no registered outbound claim at all. Endpoint inspection must
    // neither open/authenticate its QUIC pool nor reject this real factory.
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{
                "type":"hysteria2", "server":"127.0.0.1", "port":443,
                "password":"test-password", "insecure":true
            }}], "route":{"final":{"type":"route","outbound":"node"}}
        })
        .to_string(),
    )
    .unwrap();
    let proxy = crate::Proxy::new(config).unwrap();
    assert_eq!(
        probe_target_endpoint(&proxy, &proxy.engine().runtime_snapshot(), "node").unwrap(),
        Some(("127.0.0.1".into(), 443)),
    );
}

#[tokio::test]
async fn standard_outbound_probe_builtins_keep_no_fixed_endpoint() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{"outbounds":[{"tag":"local","protocol":{"type":"direct"}},{"tag":"deny","protocol":{"type":"block"}}],"route":{"final":{"type":"direct"}}}"#,
    ).unwrap();
    let proxy = crate::Proxy::new(config).unwrap();
    let snapshot = proxy.engine().runtime_snapshot();
    for tag in ["local", "deny"] {
        assert_eq!(probe_target_endpoint(&proxy, &snapshot, tag).unwrap(), None);
    }
}
