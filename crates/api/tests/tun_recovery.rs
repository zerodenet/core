use zero_api::{CommandRequest, Permission, TunRecoverCommand};

#[test]
fn tun_recovery_is_an_admin_command_with_empty_object_parameters() {
    let value = serde_json::json!({"method": "tun.recover", "params": {}});
    let request: CommandRequest = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(request, CommandRequest::TunRecover(TunRecoverCommand {}));
    assert_eq!(request.required_permission(), Permission::Admin);
    assert_eq!(serde_json::to_value(request).unwrap(), value);
}
