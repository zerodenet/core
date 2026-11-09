//! Actual Notify wake re-enters the same Source and Host commit gate.
use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
};
struct Reenter {
    source: Arc<ConfigurationSource>,
    host: Arc<crate::RuntimeHost>,
    wakes: AtomicUsize,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.source.observation();
        self.host.apply().commit(|| Ok(())).unwrap();
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn refund_notifications_run_after_original_source_and_actual_host_publication_guards() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("zero.json");
    std::fs::write(&path, b"actual previous source").unwrap();
    let source = Arc::new(ConfigurationSource::new(&path));
    let host = Arc::new(crate::RuntimeHost::<()>::new(
        crate::component::ComponentRegistry::default(),
    ));
    let book = host.control_budget();
    source.bind_persistence_budget(&book).unwrap();
    let wake = Arc::new(Reenter {
        source: source.clone(),
        host: host.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut changed = std::pin::pin!(book.capacity_changed());
    assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Pending));
    let notifications = source.defer_persistence_notifications();
    host.apply()
        .commit(|| {
            source
                .persist_ready_with_budget(&config(), "actual-publication", &book, None, 1024)
                .map_err(|error| error.to_string())?;
            assert_eq!(wake.wakes.load(Ordering::SeqCst), 0);
            Ok(())
        })
        .unwrap();
    assert_eq!(wake.wakes.load(Ordering::SeqCst), 0);
    drop(notifications);
    assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(changed.as_mut().poll(&mut cx), Poll::Ready(())));
    let mut final_change = std::pin::pin!(book.capacity_changed());
    assert!(matches!(final_change.as_mut().poll(&mut cx), Poll::Pending));
    let notifications = source.defer_persistence_notifications();
    host.apply()
        .commit(|| {
            source
                .finalize_committed()
                .map_err(|error| error.to_string())
        })
        .unwrap();
    assert_eq!(wake.wakes.load(Ordering::SeqCst), 1);
    drop(notifications);
    assert_eq!(wake.wakes.load(Ordering::SeqCst), 2);
    assert!(matches!(
        final_change.as_mut().poll(&mut cx),
        Poll::Ready(())
    ));
    assert_eq!(book.usage().response_bytes, 0);
    closed(&source);
}
