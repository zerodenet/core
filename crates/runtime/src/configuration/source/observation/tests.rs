//! The actual original metadata allocation is borrowed, never copied first.
use super::*;
#[test]
fn write_observation_keeps_its_original_reason_and_read_guard_without_opening_resources() {
    let source = ConfigurationSource::default();
    let mut reason = String::with_capacity(32768);
    reason.push_str("same original write outcome 雪");
    let address = reason.as_ptr();
    source.state.lock().unwrap().observation.reason = Some(reason);
    {
        let view = source.read_write_observation();
        assert_eq!(view.reason().unwrap().as_ptr(), address);
        assert_eq!(view.writable(), None);
        assert_eq!(view.observed_at_unix_ms(), None);
        assert!(matches!(
            source.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(source.io.ensure_available().is_ok());
    }
    assert!(source.state.try_lock().is_ok());
    assert_eq!(
        source.observation().reason.as_deref(),
        Some("same original write outcome 雪")
    );
}
