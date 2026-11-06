//! Original cancellation wakes may synchronously inspect their own real pool.
use super::*;
use std::{
    sync::{
        Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Wake, Waker},
    time::Duration,
};
struct Reenter {
    owner: Weak<DriverOwner>,
    calls: AtomicUsize,
    held: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if let Some(owner) = self.owner.upgrade() {
            if owner.state.try_lock().is_err() {
                self.held.store(true, Ordering::Release);
            }
        }
    }
}
#[tokio::test]
async fn original_driver_retirement_wakes_only_after_releasing_the_same_pool_state() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for whole_pool in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let socket = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (peer, _) = listener.accept().await.unwrap();
            let mut peer = Some(peer);
            let (sender, connection) = h2::client::handshake::<_, Bytes>(socket).await.unwrap();
            let egress = zero_platform_tokio::EgressInterfaceControl::default();
            let owner = Arc::new(DriverOwner::new(Arc::default(), Arc::default(), 1));
            let client = owner.client(None, true, egress.clone(), egress.generation());
            owner.spawn(client.clone(), connection).unwrap();
            let reenter = Arc::new(Reenter {
                owner: Arc::downgrade(&owner),
                calls: AtomicUsize::new(0),
                held: AtomicBool::new(false),
            });
            let wake = Waker::from(reenter.clone());
            let mut context = Context::from_waker(&wake);
            let stopped = client.cancel.cancelled();
            tokio::pin!(stopped);
            assert!(std::future::Future::poll(stopped.as_mut(), &mut context).is_pending());
            assert!(!client.cancel.is_cancelled());
            if whole_pool {
                owner.retire();
            } else {
                // Real peer EOF consumes this same original h2 driver exit;
                // ordinary ensure_open's reap must also wake outside the lock.
                drop(peer.take());
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        owner.ensure_open().unwrap();
                        if client.cancel.is_cancelled() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            assert!(reenter.calls.load(Ordering::Acquire) > 0);
            assert!(
                !reenter.held.load(Ordering::Acquire),
                "actual cancellation waker re-entered while its owner mutex was held"
            );
            drop(sender);
            owner
                .shutdown(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert!(owner.drained());
            assert!(owner.state.lock().unwrap().tasks.is_empty());
            if whole_pool {
                // The actual canceled driver returned its real socket view.
                use tokio::io::AsyncReadExt;
                let mut remaining = Vec::new();
                peer.as_mut()
                    .unwrap()
                    .read_to_end(&mut remaining)
                    .await
                    .unwrap();
            }
        }
    })
    .await
    .unwrap();
}
