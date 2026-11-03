use super::*;
#[test]
fn published_users_cannot_mutate_the_frozen_recipe_used_for_recovery() {
    let store = Hysteria2InboundProfileStore::default();
    let recipe = Hysteria2InboundProfile::from_config("original");
    let live = store.publish("hy", &recipe);
    live.replace_config_users([Hysteria2InboundUserRef {
        password: "updated",
        principal_key: None,
        up_bps: None,
        down_bps: None,
        device_limit: None,
        quota_remaining_bytes: None,
        policy_revision: None,
    }]);
    assert!(live.authenticate_password("updated").is_ok());
    assert!(recipe.authenticate_password("original").is_ok());
    assert!(recipe.authenticate_password("updated").is_err());
    let recovered = store.publish("hy", &recipe);
    assert!(recovered.authenticate_password("original").is_ok());
    assert!(
        live.authenticate_password("original").is_ok(),
        "recovery publishes into the same actual authority"
    );
    assert!(live.authenticate_password("updated").is_err());
}
