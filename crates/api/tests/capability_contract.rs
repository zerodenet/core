use zero_api::{
    ApiCapabilities, ApiErrorCode, CAPABILITIES_CONTRACT_VERSION, CONFIG_SCHEMA_VERSION,
    CONTROL_API_VERSION, ERROR_CODE_CONTRACT_VERSION,
};

#[test]
fn current_capabilities_freeze_v1_contract_ranges_and_error_codes() {
    let capabilities = ApiCapabilities::new();
    let contracts = capabilities
        .contracts
        .expect("current core must publish a contract manifest");

    assert_eq!(
        contracts.capabilities.current,
        CAPABILITIES_CONTRACT_VERSION
    );
    assert_eq!(contracts.capabilities.minimum_supported, 1);
    assert_eq!(contracts.control_api.current, CONTROL_API_VERSION);
    assert_eq!(contracts.control_api.minimum_supported, 1);
    assert_eq!(contracts.config_schema.current, CONFIG_SCHEMA_VERSION);
    assert_eq!(contracts.config_schema.minimum_supported, 1);
    assert_eq!(contracts.error_codes.current, ERROR_CODE_CONTRACT_VERSION);
    assert_eq!(contracts.error_codes.minimum_supported, 1);

    assert_eq!(
        capabilities.error_codes,
        ApiErrorCode::ALL
            .iter()
            .map(|code| code.as_code_str().to_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        capabilities.error_codes,
        [
            "not_found",
            "invalid_argument",
            "permission_denied",
            "insufficient_os_privilege",
            "feature_disabled",
            "conflict",
            "unsupported",
            "internal",
        ]
    );
}

#[test]
fn older_capability_payload_keeps_missing_contract_distinguishable() {
    let legacy = serde_json::json!({
        "api_id": "zero.api.v1",
        "schema_id": "zero.event.v1",
        "features": ["query"]
    });
    let parsed: ApiCapabilities = serde_json::from_value(legacy).expect("parse legacy capability");

    assert_eq!(parsed.contracts, None);
    assert!(parsed.error_codes.is_empty());
    assert!(parsed.global_limitations.is_empty());
}
