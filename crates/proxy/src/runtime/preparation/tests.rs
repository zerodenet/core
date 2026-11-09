use super::*;
#[tokio::test]
async fn bootstrap_and_proxy_use_the_same_original_root_and_real_identity_issuer() {
    let preparation = ProxyPreparation::new();
    let config = RuntimeConfig::parse(
        r#"{"runtime":{"carrier_max_total_originals":2,"component_total_max_tasks":3},"route":{"final":{"type":"direct"}}}"#,
    )
    .unwrap();
    preparation
        .configure_original_limit(&config.runtime)
        .unwrap();
    let (bootstrap_instance, bootstrap_root) = preparation.application_network_originals().unwrap();
    assert_eq!(bootstrap_root.snapshot().limit.get(), 2);
    assert_eq!(bootstrap_root.snapshot().charged, 0);
    let tasks = preparation.application_task_budget();
    assert_eq!(tasks.usage().limit, 3);
    let proxy = Proxy::from_preparation(Engine::new(config).unwrap(), preparation).unwrap();
    assert!(Arc::ptr_eq(&tasks, &proxy.application_task_budget()));
    let (application_instance, application_root) = proxy.application_network_originals().unwrap();
    assert!(Arc::ptr_eq(&bootstrap_root, &application_root));
    assert_ne!(bootstrap_instance.component, application_instance.component);
    assert_eq!(application_root.snapshot().limit.get(), 2);
    assert_eq!(
        application_root.snapshot().charged,
        0,
        "cold assembly never opens or charges an original"
    );
}
