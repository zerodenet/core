use super::*;
use crate::contract::{lifecycle::CloseReport, CancellationToken, CloseReason};
use std::sync::Mutex;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use zero_core::{
    component::{EndpointId, ResourceGeneration},
    packet::{PacketIngress, ReturnAssociation},
};

fn packet() -> IpPacket {
    let mut bytes = vec![0; 20];
    bytes[0] = 0x45;
    bytes[2..4].copy_from_slice(&20u16.to_be_bytes());
    bytes[8] = 64;
    IpPacket::new(
        bytes.into(),
        ReturnAssociation {
            endpoint: EndpointId(4),
            generation: ResourceGeneration(7),
            peer: None,
            correlation: 99,
        },
    )
    .unwrap()
    .with_ingress(PacketIngress {
        local_destination: true,
        mtu: 1500,
    })
}
fn limits() -> PacketLimits {
    PacketLimits {
        mtu: 1500,
        families: vec![IpFamily::Ipv4],
        source_adaptation: SourceAdaptation::Preserved,
        return_association: ReturnGuarantee::Flow,
    }
}
struct Unsupported;
impl PacketTx for Unsupported {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        panic!("owned send must never borrow-copy fallback")
    }
}
struct Control {
    stop: CancellationToken,
    // This is the real original join, not a body/oneshot completion notification.
    join: Arc<tokio::sync::Mutex<Option<JoinHandle<usize>>>>,
    returned: Arc<Mutex<Option<usize>>>,
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.stop.cancel();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            let mut join = self.join.lock().await;
            if let Some(original) = join.as_mut() {
                // Borrow the same original across await; cancelled close keeps it.
                let result = tokio::time::timeout_at(end, original)
                    .await
                    .map_err(|cause| {
                        ChannelError::new(Stage::Close, Reason::Timeout, Impact::Session)
                            .with_source(cause)
                    })?
                    .map_err(|cause| {
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Session)
                            .with_source(cause)
                    })?;
                *self.returned.lock().unwrap() = Some(result);
                *join = None;
            }
            Ok(CloseReport::default())
        })
    }
}
fn control() -> Control {
    Control {
        stop: CancellationToken::new(),
        join: Arc::new(tokio::sync::Mutex::new(None)),
        returned: Arc::new(Mutex::new(None)),
    }
}
#[tokio::test]
async fn rejects_before_submission_with_same_buffer_and_metadata() {
    let mut control = control();
    for invalid in [false, true] {
        let original = packet();
        let pointer = original.payload().as_ptr() as usize;
        let mut limits = limits();
        if invalid {
            limits.mtu = 19;
        }
        let failure = send_owned(Pin::new(&mut Unsupported), &mut control, original, &limits)
            .await
            .unwrap_err();
        assert_eq!(failure.failure.delivery, DeliveryState::NotSubmitted);
        assert_eq!(
            failure.failure.error.reason,
            if invalid {
                Reason::Capacity
            } else {
                Reason::Unsupported
            }
        );
        let returned = failure.packet.unwrap();
        assert_eq!(returned.payload().as_ptr() as usize, pointer);
        assert_eq!(returned, packet());
        assert!(!control.stop.is_cancelled());
    }
}
struct OwnedTx {
    queue: mpsc::Sender<IpPacket>,
    accepted: oneshot::Receiver<SendReceipt>,
}
impl PacketTx for OwnedTx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &IpPacket,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        panic!("must move original")
    }
    fn poll_send_owned(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        packet: &mut Option<IpPacket>,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if let Some(original) = packet.take() {
            if let Err(error) = self.queue.try_send(original) {
                *packet = Some(error.into_inner());
                return Poll::Ready(Err(SendFailure::new(
                    ChannelError::new(Stage::Send, Reason::Capacity, Impact::Packet),
                    DeliveryState::NotSubmitted,
                )));
            }
        }
        Pin::new(&mut self.accepted)
            .poll(cx)
            .map(|result| result.map_err(|_| unknown()))
    }
}
#[tokio::test]
async fn cancelled_pending_keeps_original_in_same_driver_until_real_join() {
    let mut control = control();
    let stop = control.stop.clone();
    let (queue, mut receive) = mpsc::channel::<IpPacket>(1);
    let (accepted, receipt) = oneshot::channel();
    let (observed, ready) = oneshot::channel();
    let original = packet();
    let pointer = original.payload().as_ptr() as usize;
    let join = tokio::spawn(async move {
        let original = receive.recv().await.unwrap();
        let held = original.payload().as_ptr() as usize;
        observed.send(held).unwrap();
        // Driver owns the original after the send future has been cancelled.
        stop.cancelled().await;
        assert_eq!(original, packet());
        drop(accepted);
        held
    });
    let join_id = join.id();
    *control.join.lock().await = Some(join);
    let mut tx = OwnedTx {
        queue,
        accepted: receipt,
    };
    let limits = limits();
    let mut send = Box::pin(send_owned(
        Pin::new(&mut tx),
        &mut control,
        original,
        &limits,
    ));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert_eq!(ready.await.unwrap(), pointer);
    drop(send);
    assert!(control.stop.is_cancelled());
    assert_eq!(control.join.lock().await.as_ref().unwrap().id(), join_id);
    control
        .wait_closed(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(control.join.lock().await.is_none());
    assert_eq!(*control.returned.lock().unwrap(), Some(pointer));
}
