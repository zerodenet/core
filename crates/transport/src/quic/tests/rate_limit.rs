use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct TestFactory(Arc<AtomicU64>);
struct TestController(Arc<AtomicU64>);
impl ControllerFactory for TestFactory {
    fn build(self: Arc<Self>, _: Instant, _: u16) -> Box<dyn Controller> {
        Box::new(TestController(self.0.clone()))
    }
}
impl Controller for TestController {
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        24_000
    }
    fn initial_window(&self) -> u64 {
        12_000
    }
    fn pacing_rate(&self) -> Option<u64> {
        match self.0.load(Ordering::Relaxed) {
            0 => None,
            rate => Some(rate),
        }
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self(self.0.clone()))
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn connection_ceiling_survives_negotiation_controller_clones_and_loss_compensation() {
    let rate = Arc::new(AtomicU64::new(0));
    let factory = cap_factory(Arc::new(TestFactory(rate.clone())), Some(1_000_000));
    let connection = factory.clone().build(Instant::now(), 1200);
    let other = factory.build(Instant::now(), 1200);
    assert_eq!(connection.pacing_rate(), Some(1_000_000));
    // The protocol may retrieve its own state from a snapshot after authentication.
    let state = connection
        .clone_box()
        .into_any()
        .downcast::<TestController>()
        .unwrap();
    state.0.store(800_000, Ordering::Relaxed);
    assert_eq!(connection.pacing_rate(), Some(800_000));
    rate.store(1_250_000, Ordering::Relaxed);
    assert_eq!(connection.pacing_rate(), Some(1_000_000));
    assert_eq!(other.pacing_rate(), Some(1_000_000));
    assert_eq!(connection.clone_box().pacing_rate(), Some(1_000_000));
    assert_eq!(connection.window(), 24_000);
    assert_eq!(connection.metrics().pacing_rate, Some(8_000_000));
}

#[test]
fn unlimited_connections_keep_the_original_controller() {
    for limit in [None, Some(0)] {
        let rate = Arc::new(AtomicU64::new(0));
        let connection =
            cap_factory(Arc::new(TestFactory(rate.clone())), limit).build(Instant::now(), 1200);
        assert_eq!(connection.pacing_rate(), None);
        rate.store(2_000_000, Ordering::Relaxed);
        assert_eq!(connection.pacing_rate(), Some(2_000_000));
    }
}
