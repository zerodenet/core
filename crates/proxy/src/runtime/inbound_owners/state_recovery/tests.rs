use super::*;
use crate::runtime::inbound_operation::PreparedInboundListenerOperation;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Notify},
};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::ResourceDeclaration;

struct Cold;
impl PreparedInboundListenerOperation for Cold {
    fn execute(
        self: Box<Self>,
        _: crate::runtime::route_runtime::InboundListenerRuntime,
        _: crate::protocol_registry::BoundInbound,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send>> {
        Box::pin(async { Ok(()) })
    }
}
struct State(Arc<AtomicUsize>);
impl InboundRecovery for State {
    fn prepare_restore(
        &self,
        _: InboundConfig,
        _: Option<&std::path::Path>,
    ) -> Result<PreparedInboundBind, EngineError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        PreparedInboundBind::new(
            Box::new(Cold),
            Arc::from([ResourceDeclaration::TcpListener {
                accepted: None,
                destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
                max_bind_attempts: NonZeroUsize::new(1).unwrap(),
            }]),
        )
    }
}

#[tokio::test]
async fn exact_origin_capsule_survives_interrupted_real_native_close_and_pending_child_join() {
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let config = proxy.engine().config();
    let last_good = config.inbounds[0].clone();
    let mut lease = crate::runtime::inbound_owners::bind::tests::lease(&proxy, &config, 0);
    lease.bind_tcp().unwrap();
    let (operation, mut listener, installed) = lease.finish().unwrap().promote().unwrap();
    drop(operation);
    let owner = installed.owner();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    owner
        .install_recovery(Arc::new(State(calls.clone())))
        .unwrap();
    proxy
        .inbound_owners
        .retain_recovery("first", &last_good)
        .unwrap();
    let pending = Arc::new(Notify::new());
    let suspended = pending.clone();
    let (resume, release) = oneshot::channel();
    owner
        .spawn_driver(async move {
            let accepting = listener.accept();
            tokio::pin!(accepting);
            let result = std::future::poll_fn(|cx| match accepting.as_mut().poll(cx) {
                Poll::Pending => {
                    suspended.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await;
            assert!(result.is_err()); // Original native accept was actually revoked.
            release.await.map_err(std::io::Error::other)?;
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    tokio::time::timeout(Duration::from_secs(2), pending.notified())
        .await
        .unwrap();
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(last_good.clone(), None, true)
        .is_err());
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(last_good.clone(), None, true)
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    resume.send(()).unwrap();
    let receipt = proxy
        .inbound_owners
        .confirm_shutdown("first", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    // Native/task evidence still does not bypass actual host publication.
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(last_good.clone(), None, true)
        .is_err());
    proxy
        .inbound_owners
        .release_closed(receipt, |_| Ok(()))
        .unwrap();
    assert!(proxy.inbound_owners.owner("first").is_none());
    drop(owner); // The passive capsule is retained by the pending transition.
    let cold = proxy
        .inbound_owners
        .prepare_recovery_bind(last_good.clone(), None, true)
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let original_port = TcpListener::bind(address).await.unwrap();
    assert_eq!(original_port.local_addr().unwrap(), address);
    let mut wrong = last_good.clone();
    wrong.listen.port = address.port();
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(wrong, None, true)
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(cold);
    // An unresolved re-entry retains the exact original state.
    proxy.inbound_owners.finish_recovery_transition(&config);
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(last_good, None, true)
        .unwrap()
        .is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
