use super::fixtures::*;
use std::{
    error::Error,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use zero_config::ConfigError;
use zero_core::component::ComponentRoleIds;
use zero_platform_tokio::contract::Reason;
use zero_proxy::component::{ComponentRegistry, DirectFactory, DirectOptions, RegistrationError};
use zero_traits::DialPolicy;

// Intentionally not Clone. Prepared recipes must retain typed Options, not input.
struct Input {
    policy: DialPolicy,
    dropped: Arc<AtomicUsize>,
}
impl Drop for Input {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn decode_once_and_rebuild_without_retaining_input() {
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut registry = ComponentRegistry::<Input>::default();
    let count = calls.clone();
    registry
        .register(descriptor(), DirectFactory, move |input| {
            count.fetch_add(1, Ordering::Relaxed);
            DirectOptions::validate(input.policy.clone())
                .map_err(|e| ConfigError::InvalidOutbound(e.to_string()))
        })
        .unwrap();
    let registry = registry.clone();
    let input = Input {
        policy: DialPolicy::default(),
        dropped: dropped.clone(),
    };
    let recipe = registry.prepare("direct", &input).unwrap();
    drop(input);
    assert_eq!(dropped.load(Ordering::Relaxed), 1);
    for recipe in [recipe.clone(), recipe] {
        let (context, mut scope) = build_context();
        let mut instance = recipe.build(context).unwrap();
        assert!(instance.roles.stream.is_some());
        assert!(instance.roles.datagram.is_none());
        instance.lifecycle.start(deadline()).await.unwrap();
        assert!(instance
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(registry.descriptors().count(), 1);
}

#[test]
fn unknown_disabled_duplicate_and_feature_mismatch_are_distinct() {
    let mut registry = ComponentRegistry::<DialPolicy>::default();
    assert!(matches!(
        registry.prepare("typo", &Default::default()),
        Err(RegistrationError::Unknown(_))
    ));
    registry.declare("direct", "another-feature").unwrap();
    assert!(matches!(
        registry.prepare("direct", &Default::default()),
        Err(RegistrationError::FeatureDisabled {
            feature: "another-feature",
            ..
        })
    ));
    assert!(matches!(
        registry.register(descriptor(), DirectFactory, |_| Ok(
            DirectOptions::validate(Default::default()).unwrap()
        )),
        Err(RegistrationError::FeatureMismatch("direct"))
    ));
    assert!(matches!(
        registry.declare("direct", "core"),
        Err(RegistrationError::Duplicate("direct"))
    ));
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(descriptor(), DirectFactory, |_| {
            Ok(DirectOptions::validate(Default::default()).unwrap())
        })
        .unwrap();
    assert!(matches!(
        registry.register(descriptor(), DirectFactory, |_| Ok(
            DirectOptions::validate(Default::default()).unwrap()
        )),
        Err(RegistrationError::Duplicate("direct"))
    ));
}

#[test]
fn option_errors_preserve_field_path_and_source() {
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(descriptor(), DirectFactory, |_| {
            Err(ConfigError::InvalidOutbound(
                "outbounds[2].dial.source_ip: must be local".into(),
            ))
        })
        .unwrap();
    let error = registry.prepare("direct", &()).err().unwrap();
    assert!(error.to_string().contains("outbounds[2].dial.source_ip"));
    assert!(error
        .source()
        .unwrap()
        .downcast_ref::<ConfigError>()
        .is_some());
}

#[tokio::test]
async fn missing_host_identity_is_rejected_before_build() {
    let mut registry = ComponentRegistry::<()>::default();
    registry
        .register(descriptor(), DirectFactory, |_| {
            Ok(DirectOptions::validate(Default::default()).unwrap())
        })
        .unwrap();
    let recipe = registry.prepare("direct", &()).unwrap();
    let (mut context, _scope) = build_context();
    context.roles = ComponentRoleIds::default();
    let error = recipe.build(context).err().unwrap();
    assert_eq!(error.reason, Reason::Internal);
    assert!(error.to_string().contains("Prepare") || error.source().is_some());
}
