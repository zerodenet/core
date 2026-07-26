use super::*;
use zero_config::ConfigError;

#[test]
fn config_error_details_carry_field_path() {
    let api = config_error_to_api(ConfigError::EmptyTag { scope: "inbound" });
    assert_eq!(api.code, ApiErrorCode::InvalidArgument);
    assert_eq!(api.details.len(), 1);
    assert_eq!(api.details[0].field_path.as_deref(), Some("inbound"));
    assert!(api.details[0].message.contains("tag must not be empty"));

    let api = config_error_to_api(ConfigError::DuplicateInboundListen {
        address: "0.0.0.0".to_owned(),
        port: 1080,
    });
    assert_eq!(api.details[0].field_path.as_deref(), Some("inbounds"));

    let api = config_error_to_api(ConfigError::DuplicateTag {
        scope: "outbound",
        tag: "dup".to_owned(),
    });
    assert_eq!(api.details[0].field_path.as_deref(), Some("outbound"));
    assert!(api.details[0].message.contains("`dup`"));
}

#[test]
fn config_error_invalid_variant_extracts_field_token() {
    let api = config_error_to_api(ConfigError::InvalidInbound(
        "inbounds[0] `socks-in`: password must not be empty".to_owned(),
    ));
    assert_eq!(api.details[0].field_path.as_deref(), Some("inbounds[0]"));
    assert!(api.details[0]
        .message
        .contains("password must not be empty"));

    let api = config_error_to_api(ConfigError::InvalidRuntime(
        "`runtime.udp_upstream_idle_timeout_seconds` must be greater than 0".to_owned(),
    ));
    assert_eq!(
        api.details[0].field_path.as_deref(),
        Some("runtime.udp_upstream_idle_timeout_seconds")
    );
}

#[test]
fn invalid_config_field_path_extracts_leading_token() {
    assert_eq!(
        invalid_config_field_path("inbounds[0] `tag`: bad"),
        Some("inbounds[0]".to_owned())
    );
    assert_eq!(
        invalid_config_field_path("dns route 1: domain must not be empty"),
        Some("dns".to_owned())
    );
    assert_eq!(invalid_config_field_path("no separator here"), None);
}

#[test]
fn details_serialize_when_present() {
    let api = config_error_to_api(ConfigError::EmptyTag { scope: "inbound" });
    let json = serde_json::to_value(&api).expect("serialize");
    assert!(json.get("details").is_some());
    assert_eq!(json["details"][0]["field_path"], "inbound");

    let plain = ApiError::new(ApiErrorCode::Internal, "boom");
    let json = serde_json::to_value(&plain).expect("serialize");
    assert!(json.get("details").is_none());
}
