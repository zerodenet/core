//! Immutable recipe lifetime does not create or retain a native provider.
use super::*;
#[test]
fn accepted_browser_recipe_survives_registry_refresh_until_its_actual_last_guard_returns() {
    let registry = Registry::default();
    let settings = zero_traits::BrowserDialerSettings {
        listen: "127.0.0.1:16888".into(),
        idle_capacity: 1,
        task_timeout_ms: 2_000,
        max_task_bytes: 64 * 1024,
        max_payload_bytes: 1024 * 1024,
    };
    let original = registry.access(settings.clone());
    let recipe = ProviderOptions::new(original.clone()).unwrap();
    assert_eq!(original.0.recipes.load(Ordering::Acquire), 1);
    assert!(!original.0.hosted.load(Ordering::Acquire));
    assert!(original.0.server.try_lock().unwrap().is_none());
    registry.retire();
    assert!(!original.0.retired.load(Ordering::Acquire));
    assert!(Arc::ptr_eq(
        &registry.access(settings.clone()).0,
        &original.0
    ));
    // Last-good rebuild can select the same accepted recipe. This does not
    // claim that any provider has started or that a Source is now closed.
    let recovery = ProviderOptions::new(original.clone()).unwrap();
    drop(recipe);
    registry.retire();
    assert_eq!(original.0.recipes.load(Ordering::Acquire), 1);
    assert!(!original.0.retired.load(Ordering::Acquire));
    drop(recovery);
    registry.retire();
    assert_eq!(original.0.recipes.load(Ordering::Acquire), 0);
    assert!(original.0.retired.load(Ordering::Acquire));
    assert!(!Arc::ptr_eq(&registry.access(settings).0, &original.0));
}
