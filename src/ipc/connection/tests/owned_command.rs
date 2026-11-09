use super::super::*;
fn old(method: String, params: serde_json::Value) -> Result<CommandRequest, CommandParseError> {
    let mut wrapper = serde_json::Map::new();
    wrapper.insert("method".into(), serde_json::Value::String(method));
    wrapper.insert("params".into(), params);
    serde_json::from_value(serde_json::Value::Object(wrapper)).map_err(|error| {
        Box::new(ApiResponse::error_msg(
            "invalid_argument",
            error.to_string(),
        ))
    })
}
#[test]
fn actual_owned_command_fields_keep_original_api_decode_semantics_without_wrapper_map() {
    for (method, params) in [
        (
            "mode.set",
            serde_json::json!({"mode":"global","outbound":"same-target"}),
        ),
        (
            "config.apply",
            serde_json::json!({"config":{"outbounds":[{"tag":"d","protocol":{"type":"direct"}}]}}),
        ),
        (
            "tun.start",
            serde_json::json!({"addr":"10.0.0.1","tag":"tun","include_cidrs":["10.0.0.0/8"]}),
        ),
        ("future.command", serde_json::json!({"future":true})),
        ("mode.set", serde_json::json!({"mode":9})),
        ("tun.recover", serde_json::Value::Null),
    ] {
        let expected = old(method.into(), params.clone());
        let actual = parse_owned_command(method.into(), params);
        match (expected, actual) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
            (Err(expected), Err(actual)) => assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            ),
            _ => panic!("actual original command decode changed for {method}"),
        }
    }
}
#[test]
fn actual_parameter_strings_move_to_the_original_typed_command_without_cloning() {
    let policy = String::from("same-policy");
    let target = String::from("same-target");
    let (policy_ptr, target_ptr) = (policy.as_ptr(), target.as_ptr());
    let mut params = serde_json::Map::new();
    params.insert("policy_tag".into(), serde_json::Value::String(policy));
    params.insert("target_tag".into(), serde_json::Value::String(target));
    let CommandRequest::PolicySelect(command) =
        parse_owned_command("policies.select".into(), serde_json::Value::Object(params)).unwrap()
    else {
        panic!("exact original command")
    };
    assert_eq!(command.policy_tag.as_ptr(), policy_ptr);
    assert_eq!(command.target_tag.as_ptr(), target_ptr);
}
