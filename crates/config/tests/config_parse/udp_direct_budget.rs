use zero_config::RuntimeConfig;

#[test]
fn accepted_udp_native_budget_is_optional_finite_and_preserves_default_export() {
    let parse = |limit: Option<usize>| {
        let mut raw = serde_json::json!({"route":{"final":{"type":"direct"}}});
        if let Some(limit) = limit {
            raw["runtime"] = serde_json::json!({"udp_direct_max_sockets":limit});
        }
        RuntimeConfig::parse(&raw.to_string())
    };
    let legacy = parse(None).unwrap();
    assert_eq!(legacy.runtime.effective_udp_direct_max_sockets(), 64);
    assert!(serde_json::to_value(&legacy.runtime)
        .unwrap()
        .get("udp_direct_max_sockets")
        .is_none());
    for limit in [1, 17, 4096] {
        let config = parse(Some(limit)).unwrap();
        assert_eq!(config.runtime.effective_udp_direct_max_sockets(), limit);
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["runtime"]["udp_direct_max_sockets"], limit);
        assert_eq!(
            RuntimeConfig::parse(&encoded.to_string())
                .unwrap()
                .runtime
                .udp_direct_max_sockets,
            Some(limit)
        );
    }
    for limit in [0, 4097, usize::MAX] {
        assert!(parse(Some(limit)).is_err());
    }
}
