//! Actual pending pipe IO can synchronously re-enter its original registry.
use super::*;
use std::{
    pin::Pin,
    sync::{atomic::AtomicUsize, Weak},
    task::{Context, Wake, Waker},
};
use tokio::{
    io::{AsyncRead, ReadBuf},
    net::windows::named_pipe::ClientOptions,
};
struct Reenter {
    originals: Weak<Originals>,
    wakes: AtomicUsize,
    held_registry: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let originals = self.originals.upgrade().unwrap();
        if originals.0.try_lock().is_err() {
            self.held_registry.store(true, Ordering::Release);
        }
        self.wakes.fetch_add(1, Ordering::AcqRel);
    }
}
#[tokio::test]
async fn stopping_real_pending_pipe_wakes_after_releasing_original_registry() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut scope = TaskScope::suspended_hosted(
            tokio::runtime::Handle::current(),
            std::num::NonZeroUsize::new(1),
        );
        scope.enable_registration().unwrap();
        let originals = Arc::new(Originals::default());
        let budget = zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        );
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = PathBuf::from(format!(
            r"\\.\pipe\zero-stop-waker-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::AcqRel)
        ));
        let entry = originals
            .prepare(&path, scope.registrar(), &budget)
            .unwrap();
        let mut view = entry.original.open().await.unwrap();
        let client = ClientOptions::new().open(&path).unwrap();
        view.connect().await.unwrap();
        while !entry.original.pending_io().1 {
            tokio::task::yield_now().await;
        }
        let wake = Arc::new(Reenter {
            originals: Arc::downgrade(&originals),
            wakes: AtomicUsize::new(0),
            held_registry: AtomicBool::new(false),
        });
        let waker = Waker::from(wake.clone());
        let mut byte = [0];
        let mut output = ReadBuf::new(&mut byte);
        assert!(Pin::new(&mut view)
            .poll_read(&mut Context::from_waker(&waker), &mut output)
            .is_pending());
        originals.request_stop();
        assert!(wake.wakes.load(Ordering::Acquire) > 0);
        assert!(!wake.held_registry.load(Ordering::Acquire));
        assert!(entry.original.closed().is_none());
        assert!(!entry.original.view_returned());
        assert_eq!(budget.usage().connections, 1);
        let worker = entry.original.worker().unwrap();
        drop(view);
        let receipt = scope.next_receipt().await.unwrap();
        assert!(worker.matches_joined(receipt.joined()));
        receipt.joined().result().unwrap();
        assert!(!receipt.joined().registration_released());
        let closed = entry
            .original
            .close(super::super::super::owner::close_deadline())
            .await
            .unwrap();
        assert!(entry.original.matches_closed(&closed) && closed.had_native_acquisition());
        assert!(entry.original.std_worker_joined());
        assert_eq!(
            originals.reap(true).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(budget.usage().connections, 1);
        scope.acknowledge(&receipt).unwrap();
        originals.reap(true).await.unwrap();
        assert!(originals.is_empty());
        assert_eq!(budget.usage().connections, 0);
        assert_eq!(budget.usage().pipe_workspace_bytes, 0);
        budget.close_admission();
        budget.confirm_released().unwrap();
        drop(client);
    })
    .await
    .unwrap();
}
