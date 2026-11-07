use super::*;
use std::sync::{atomic::Ordering, mpsc};
fn registry(limits: SubscriberLimits) -> Arc<Mutex<SubscriberRegistry>> {
    let mut original = SubscriberRegistry::default();
    original.check_limits(limits).unwrap();
    original.install_limits(limits);
    Arc::new(Mutex::new(original))
}
fn subscribe(
    registry: &Arc<Mutex<SubscriberRegistry>>,
    filter: EventFilter,
) -> (mpsc::Receiver<QueuedEvent>, SubscriberRegistration) {
    let prepared = PreparedSubscriber::prepare(registry, filter).unwrap();
    let (tx, rx) = mpsc::sync_channel(1024);
    let registration = prepared.install(tx).unwrap();
    (rx, registration)
}
fn event() -> Arc<RawApiEvent> {
    Arc::new(ApiEvent::new(
        "subscriber-custody",
        "same.event",
        1,
        json!({"value": 7}),
    ))
}
fn send(registry: &Arc<Mutex<SubscriberRegistry>>, bytes: usize) {
    registry.lock().unwrap().send(&event(), bytes);
}
mod custody;
mod storage;

mod configuration;
