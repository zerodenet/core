use super::*;
use futures_util::task::AtomicWaker;
use std::{
    io,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Context,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Probe {
    polls: AtomicUsize,
    dropped: AtomicBool,
    wake: AtomicWaker,
    changed: Notify,
}
struct PendingUpload(Arc<Probe>);
impl Drop for PendingUpload {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::Release);
    }
}
impl hyper::body::Body for PendingUpload {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, io::Error>>> {
        self.0.wake.register(cx.waker());
        self.0.polls.fetch_add(1, Ordering::AcqRel);
        self.0.changed.notify_one();
        Poll::Pending
    }
}
impl Probe {
    async fn polled_after(&self, previous: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.polls.load(Ordering::Acquire) > previous {
                return;
            }
            changed.await;
        }
    }
}
#[tokio::test]
async fn dropping_the_last_sender_keeps_the_real_pending_upload_delegate_until_owner_cancellation()
{
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::new().await;
        let (carrier, peer) = fixture.connected().await;
        let adoption = carrier.adoption().clone();
        let mut scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let cancel = CancellationToken::new();
        let mut pending = spawn::<PendingUpload>(
            carrier,
            &scope.registrar(),
            TaskClass::Auxiliary,
            options(4, cancel.clone()),
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let mut h2 = h2::server::handshake(peer).await.unwrap();
            let (request, mut respond) = h2.accept().await.unwrap().unwrap();
            let response = respond
                .send_response(http::Response::new(()), false)
                .unwrap();
            while let Some(result) = h2.accept().await {
                if result.is_err() {
                    break;
                }
            }
            drop((request, response));
        });
        let probe = Arc::new(Probe::default());
        let mut sender = pending.ready().await.unwrap();
        let response = sender
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri("https://fixture.test/held-upload")
                    .body(PendingUpload(probe.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        probe.polled_after(0).await;
        drop(sender);
        // No raw sender remains in PendingH2 after ready. The original outer
        // dispatcher observes Sender dropped, while its original Pipe still
        // owns this real upload body. Wake that Pipe and observe another poll.
        let previous = probe.polls.load(Ordering::Acquire);
        probe.wake.wake();
        probe.polled_after(previous).await;
        assert!(!probe.dropped.load(Ordering::Acquire));
        let mut joining = Box::pin(pending.join_witness().wait());
        pending_once(joining.as_mut()).await;
        drop(joining);
        assert!(adoption.closed().is_none());
        assert_eq!(fixture.owner.retained(), 1);
        cancel.cancel();
        assert_eq!(scope.next_exit().await.unwrap().id, pending.task_id());
        assert!(probe.dropped.load(Ordering::Acquire));
        assert!(adoption.closed().is_none());
        assert!(adoption.retire(end()).await.unwrap().is_complete());
        assert_eq!(fixture.owner.retained(), 0);
        drop(response);
        server.await.unwrap();
        assert!(scope.shutdown(end()).await.is_complete());
        assert!(fixture.owner.shutdown(end()).await.unwrap().is_complete());
    })
    .await
    .unwrap();
}
