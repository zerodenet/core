use super::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Weak,
    },
    task::{Context, Wake, Waker},
};
struct Reenter {
    host: Weak<HostResources>,
    wakes: AtomicUsize,
    busy: AtomicBool,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        let host = self.host.upgrade().unwrap();
        let proof = host.io_evidence();
        self.busy.store(proof.metadata[0].busy, Ordering::SeqCst);
        assert!(proof.admission_closed);
        let failed = host.bind_metadata(request(loopback())).err().unwrap();
        assert_eq!(failed.reason, Reason::Revoked);
        // Exercise the real async shutdown entry while the same native close
        // call is flushing this callback. Its busy entry is never a close ACK.
        let mut shutdown = Box::pin(host.shutdown_io(deadline()));
        let result = shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()));
        assert!(matches!(result,std::task::Poll::Ready(Ok(report)) if !report.is_complete()));
    }
}
#[tokio::test]
async fn original_pending_receive_wake_can_reenter_host_observation_and_shutdown_outside_parent_locks(
) {
    let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
    let host = resources(&scope, loopback(), 1);
    let _access = host.activate(deadline()).unwrap();
    let mut grant = host
        .listen_metadata_datagram(request(loopback()))
        .await
        .unwrap();
    let local = grant.tx.local_addr().unwrap();
    let callback = Arc::new(Reenter {
        host: Arc::downgrade(&host),
        wakes: AtomicUsize::new(0),
        busy: AtomicBool::new(false),
    });
    let waker = Waker::from(callback.clone());
    let mut bytes = [0u8; 16];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut metadata = [MetadataReceive::default()];
    assert!(grant
        .rx
        .poll_recv(
            &mut Context::from_waker(&waker),
            &mut buffers,
            &mut metadata
        )
        .is_pending());
    let report = host.shutdown_io(deadline()).await.unwrap();
    assert!(!report.is_complete());
    assert!(callback.wakes.load(Ordering::SeqCst) > 0);
    assert!(callback.busy.load(Ordering::SeqCst));
    let actual = host.io_evidence();
    assert_eq!(actual.metadata[0].main.confirmed_closed, 1);
    assert_eq!(actual.metadata[0].main.live, 0);
    assert!(actual.metadata[0].main.unknown.is_empty());
    let retained = std::net::UdpSocket::bind(local).unwrap();
    assert!(scope.shutdown(deadline()).await.is_complete());
    drop(retained);
}
