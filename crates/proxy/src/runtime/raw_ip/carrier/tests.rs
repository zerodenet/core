use super::*;
use std::{future::Future, sync::Mutex, task::Poll};
use tokio::sync::oneshot;

struct UnusedPath;
#[async_trait]
impl PacketPathCarrier for UnusedPath {
    async fn send_to(&self, _: &Address, _: u16, _: &[u8]) -> Result<(), EngineError> {
        panic!("unused original path send")
    }
    async fn recv_from(&self, _: &mut [u8]) -> Result<usize, EngineError> {
        panic!("unused original path receive")
    }
}
struct PendingBuild {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    original: Mutex<Option<Arc<tokio::net::UdpSocket>>>,
}
impl PreparedUdpPacketPathOperation for PendingBuild {
    fn build_carrier<'a>(
        &'a self,
        _: PacketPathExecutionServices,
    ) -> crate::runtime::udp_dispatch::packet_path_operation::PacketPathCarrierFuture<'a> {
        Box::pin(async move {
            let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await?);
            *self.original.lock().unwrap() = Some(socket.clone());
            let mut bytes = [0; 1];
            let mut pending = Box::pin(socket.recv_from(&mut bytes));
            std::future::poll_fn(|cx| {
                let result = pending.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(entered) = self.entered.lock().unwrap().take() {
                        entered.send(()).unwrap();
                    }
                }
                result
            })
            .await?;
            panic!("the real UDP receive remains pending until this original worker is cancelled");
        })
    }
}
fn services() -> (crate::runtime::Proxy, PacketPathExecutionServices) {
    let config = zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let services =
        PacketPathExecutionServices::from_tcp_execution(&proxy.tcp_runtime_services().execution());
    (proxy, services)
}

#[tokio::test]
async fn original_reconnect_udp_wait_is_retained_across_interrupted_actual_join_without_an_extra_monitor(
) {
    let (_proxy, services) = services();
    let (entered, pending) = oneshot::channel();
    let operation = Arc::new(PendingBuild {
        entered: Mutex::new(Some(entered)),
        original: Mutex::new(None),
    });
    let path = Arc::new(UnusedPath) as Arc<dyn PacketPathCarrier>;
    let carrier = ProxiedRawIpWireCarrier::new(path.clone(), operation.clone(), services);
    let weak = Arc::downgrade(&carrier.state);
    carrier.state.rebuild_if_current(path);
    pending.await.unwrap();
    let tasks = carrier.state.tasks.lock().await.unwrap();
    assert_eq!(tasks.len(), 1);
    let mut closing = carrier.stop_background().unwrap();
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(tasks.len(), 1);
    drop(tasks);
    carrier.stop_background().unwrap().await.unwrap();
    // This is deliberately not native-close evidence: the original fixture
    // still owns its real UDP socket after the original reconnect task joined.
    let original = operation.original.lock().unwrap().clone().unwrap();
    assert!(original.local_addr().unwrap().port() != 0);
    drop(carrier);
    assert!(weak.upgrade().is_none());
}

struct PanicBuild;
impl PreparedUdpPacketPathOperation for PanicBuild {
    fn build_carrier<'a>(
        &'a self,
        _: PacketPathExecutionServices,
    ) -> crate::runtime::udp_dispatch::packet_path_operation::PacketPathCarrierFuture<'a> {
        Box::pin(async { panic!("original reconnect child panic") })
    }
}
#[tokio::test]
async fn existing_receive_driver_supervision_preserves_the_original_reconnect_join_error_for_cleanup(
) {
    let (_proxy, services) = services();
    let path = Arc::new(UnusedPath) as Arc<dyn PacketPathCarrier>;
    let carrier = ProxiedRawIpWireCarrier::new(path.clone(), Arc::new(PanicBuild), services);
    carrier.state.rebuild_if_current(path);
    let error = carrier.state.tasks.wait_one().await.unwrap_err();
    let original = find_join(&error);
    assert!(original.is_panic());
    let original = original as *const tokio::task::JoinError;
    let retry = carrier.stop_background().unwrap().await.unwrap_err();
    assert_eq!(find_join(&retry) as *const tokio::task::JoinError, original);
}
fn find_join<'a>(error: &'a (dyn std::error::Error + 'static)) -> &'a tokio::task::JoinError {
    let mut error = error;
    loop {
        if let Some(original) = error.downcast_ref::<tokio::task::JoinError>() {
            return original;
        }
        error = error
            .source()
            .expect("retained original JoinError in source chain");
    }
}
