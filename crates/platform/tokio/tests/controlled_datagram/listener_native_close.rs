//! Real native close blocked on its original gate; no fabricated close result.
use super::*;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Wake, Waker},
    time::Duration,
};

#[derive(Default)]
struct Wakes(AtomicUsize);
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn same_native_close_completion_wakes_the_original_waiter_after_cancellation() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
        let (mut rx, tx, mut native) = owner
            .bind_listener(
                "127.0.0.1:0".parse().unwrap(),
                NonZeroUsize::new(1).unwrap(),
            )
            .unwrap();
        let identity = tx.identity();
        let entry = tx.shared.socket.entry.clone();
        let mut bytes = [0; 8];
        let mut receive = Box::pin(rx.recv_from_addr(&mut bytes));
        assert!(receive
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());

        // The actual close operation takes its lease, then blocks on this
        // original native handle gate. The flag proves it entered that phase.
        let (native, receipt, waiting, wakes) = {
            let gate = entry.lock();
            let closer = std::thread::spawn(move || {
                let receipt = native.close().unwrap();
                (native, receipt)
            });
            let entered_deadline = std::time::Instant::now() + Duration::from_secs(1);
            while !entry.close_running.load(Ordering::Acquire) {
                assert!(std::time::Instant::now() < entered_deadline);
                std::thread::yield_now();
            }
            assert!(entry.closed_receipt.get().is_none());
            let wakes = Arc::new(Wakes::default());
            let waker = Waker::from(wakes.clone());
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            let mut waiting = Box::pin(identity.wait_native_operation(deadline));
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            drop(waiting);
            let mut waiting = Box::pin(identity.wait_native_operation(deadline));
            assert!(waiting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            assert_eq!(wakes.0.load(Ordering::Acquire), 0);
            drop(gate);
            let (native, receipt) = closer.join().unwrap();
            (native, receipt, waiting, wakes)
        };
        assert_eq!(receipt.reservation(), native.reservation());
        assert!(
            wakes.0.load(Ordering::Acquire) > 0,
            "actual close release wakes its observer"
        );
        assert!(waiting.await);
        assert_eq!(
            receive.await.unwrap_err().kind(),
            std_io::ErrorKind::NotConnected
        );
        assert!(tx
            .send_to_addr(b"retired", "127.0.0.1:9".parse().unwrap())
            .await
            .is_err());
        let report = owner.take_report();
        assert_eq!(report.confirmed_closed, 1);
        assert!(report.is_complete());
    })
    .await
    .unwrap();
}
