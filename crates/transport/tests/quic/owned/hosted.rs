use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};
use zero_platform_tokio::contract::scope::{TaskClass, TaskScope};

#[derive(Debug)]
struct FailActualSend {
    fired: AtomicBool,
    sent: Mutex<Option<oneshot::Sender<()>>>,
}
impl zero_traits::IoObserver for FailActualSend {
    fn sent(&self, _bytes: usize) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            self.sent.lock().unwrap().take().unwrap().send(()).unwrap();
            panic!("actual Quinn connection native submission observer failed");
        }
    }
    fn received(&self, _bytes: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn actual_auxiliary_driver_failure_keeps_host_credit_until_owning_receiver_accepts_same_join()
{
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut host =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(bound(2)));
        host.enable_registration().unwrap();
        let (drivers, mut receiver) = QuinnDriverOwner::new(host.registrar(), bound(2));
        let mut native = socket();
        let (send, submitted) = oneshot::channel();
        native
            .set_observer(Some(Arc::new(FailActualSend {
                fired: AtomicBool::new(false),
                sent: Mutex::new(Some(send)),
            })))
            .unwrap();
        let endpoint = super::drivers::endpoint(&drivers, &native);
        // Keep a real UDP peer bound through the actual QUIC Initial receipt.
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let connecting = endpoint
            .connect(peer.local_addr().unwrap(), "localhost")
            .unwrap();
        let mut payload = [0; 2048];
        let (wire, ()) = tokio::join!(peer.recv_from(&mut payload), async {
            submitted.await.unwrap()
        });
        assert!(
            wire.unwrap().0 > 0,
            "failure came after a real QUIC native submission"
        );
        let receipt = host.next_receipt().await.unwrap();
        assert_eq!(receipt.class(), TaskClass::Auxiliary);
        let original = receipt.result().unwrap_err();
        assert!(!receipt.local_failure_accepted());
        assert!(!host.is_drained());
        let status = drivers.task_status();
        assert!(status.unreceived.contains(&receipt.id()));
        assert!(
            drivers.body(receipt.id()).is_none(),
            "panic is an actual native JoinError, not a manufactured DriverExit"
        );
        let delivered = receiver
            .receive(receipt.id(), Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &original,
            &delivered.joined.result().unwrap_err()
        ));
        assert!(receipt.local_failure_accepted());
        assert!(
            native.socket().local_addr().is_ok(),
            "failure reception is not native closure"
        );
        host.acknowledge(&receipt).unwrap();
        drivers.reclaim_received();
        endpoint.close(0u32.into(), b"fixture cleanup");
        drop(connecting);
        drop(endpoint);
        host.begin_shutdown();
        let endpoint_exit = host.next_receipt().await.unwrap();
        assert_eq!(endpoint_exit.class(), TaskClass::Critical);
        endpoint_exit.result().unwrap();
        receiver
            .receive(endpoint_exit.id(), Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        host.acknowledge(&endpoint_exit).unwrap();
        assert!(host.next_receipt().await.is_none());
        assert!(host.is_drained());
        drivers.reclaim_received();
        assert_eq!(drivers.task_status().retained, 0);
        assert_eq!(drivers.task_status().reclaimed, 2);
        assert_eq!(native.close_io().main.confirmed_closed, 1);
        // Original failure remains in the real receiving caller's delivery.
        assert!(Arc::ptr_eq(
            &original,
            &delivered.joined.result().unwrap_err()
        ));
    })
    .await
    .unwrap();
}
