use crate::contract_support as support;

fn require_send<T: Send>(value: T) -> T {
    value
}
use std::{
    cell::Cell,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::time::Instant;
use zero_core::{component::AssociationId, packet::Datagram, request::DatagramTargets};
use zero_platform_tokio::contract::{
    datagram::*,
    lifecycle::{ChannelControl, CloseReport, Unfinished},
    *,
};

struct Tx {
    cancelled: Arc<AtomicBool>,
    pending: bool,
}
impl DatagramTx for Tx {
    fn poll_send(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        if self.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(Err(SendFailure::new(
                ChannelError::new(Stage::Send, Reason::Revoked, Impact::Session),
                DeliveryState::Unknown,
            )));
        }
        if self.pending {
            self.pending = false;
            return Poll::Pending;
        }
        Poll::Ready(Ok(SendReceipt::accepted(frame.payload.len())))
    }
}
struct Rx {
    frames: VecDeque<Datagram>,
    _single_consumer: Cell<()>,
}
impl DatagramRx for Rx {
    fn poll_recv(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        Poll::Ready(Ok(self.frames.pop_front()))
    }
}
struct Control {
    cancelled: Arc<AtomicBool>,
    cancels: Arc<AtomicUsize>,
    _single_owner: Cell<()>,
}
impl ChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        assert_eq!(reason, CloseReason::DeliveryUnknown);
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.cancels.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn wait_closed(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport::default())
    }
}
struct Outbound {
    pending: bool,
    cancelled: Arc<AtomicBool>,
    cancels: Arc<AtomicUsize>,
}
impl DatagramOutbound for Outbound {
    type Tx = Tx;
    type Rx = Rx;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        _: context::OpenContext,
    ) -> Result<DatagramSession<Tx, Rx, Control>, ChannelError> {
        Ok(DatagramSession {
            tx: Tx {
                cancelled: self.cancelled.clone(),
                pending: self.pending,
            },
            rx: Rx {
                frames: [Datagram {
                    address: support::destination(),
                    association: request.association,
                    payload: Vec::new().into(),
                }]
                .into(),
                _single_consumer: Cell::new(()),
            },
            control: Control {
                cancelled: self.cancelled.clone(),
                cancels: self.cancels.clone(),
                _single_owner: Cell::new(()),
            },
            limits: DatagramLimits {
                max_payload: 1200,
                targets: request.targets,
                queued_bytes: 2400,
            },
        })
    }
}
async fn open(pending: bool) -> (BoxDatagramSession, Arc<AtomicUsize>) {
    let cancels = Arc::new(AtomicUsize::new(0));
    let outbound = erase_datagram(Outbound {
        pending,
        cancelled: Arc::new(AtomicBool::new(false)),
        cancels: cancels.clone(),
    });
    let session = outbound
        .open(
            DatagramRequest {
                association: AssociationId(3),
                targets: DatagramTargets::Multiple,
            },
            support::context(),
        )
        .await
        .unwrap();
    (session, cancels)
}

#[tokio::test]
async fn dynamic_session_accepts_non_sync_rx_and_control_and_delivers_empty_datagram_once() {
    let (mut session, cancels) = open(false).await;
    let frame = recv(session.rx.as_mut()).await.unwrap().unwrap();
    assert!(frame.payload.is_empty());
    assert_eq!(frame.association, AssociationId(3));
    let sent = send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap();
    assert_eq!(sent, SendReceipt::accepted(0));
    assert!(recv(session.rx.as_mut()).await.unwrap().is_none());
    assert_eq!(cancels.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_a_pending_send_isolates_the_session_and_forbids_residual_frame_reuse() {
    let (mut session, cancels) = open(true).await;
    let frame = Datagram {
        address: support::destination(),
        association: AssociationId(3),
        payload: vec![1, 2, 3].into(),
    };
    {
        let mut pending = Box::pin(require_send(send(
            session.tx.as_mut(),
            session.control.as_mut(),
            &frame,
            &session.limits,
        )));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(pending.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(cancels.load(Ordering::SeqCst), 1);
    let failure = send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    assert!(!failure.error.retry_permitted());
    assert_eq!(cancels.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropping_an_unpolled_send_does_not_claim_side_effects_or_cancel_the_session() {
    let (mut session, cancels) = open(true).await;
    let frame = Datagram {
        address: support::destination(),
        association: AssociationId(3),
        payload: Vec::new().into(),
    };
    drop(send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    ));
    assert_eq!(cancels.load(Ordering::SeqCst), 0);
}

#[test]
fn close_report_with_unconfirmed_io_does_not_authorize_resource_replacement() {
    let mut report = CloseReport::default();
    report.append(CloseReport {
        unfinished: vec![Unfinished::Io("listener".into())],
        ..Default::default()
    });
    assert!(!report.is_complete());
}

#[tokio::test]
async fn oversized_and_wrong_target_datagrams_are_rejected_before_submission() {
    let (mut session, cancels) = open(false).await;
    let mut frame = Datagram {
        address: support::destination(),
        association: AssociationId(3),
        payload: vec![0; 1201].into(),
    };
    let error = send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.reason, Reason::Capacity);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    frame.payload = Vec::new().into();
    session.limits.targets = DatagramTargets::Fixed(frame.address.clone());
    frame.address.port = 80;
    let error = send(
        session.tx.as_mut(),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(error.error.reason, Reason::Protocol);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert_eq!(cancels.load(Ordering::SeqCst), 0);
}

struct PartialSuccess;
impl DatagramTx for PartialSuccess {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(Ok(SendReceipt::accepted(1)))
    }
}
#[tokio::test]
async fn partial_success_is_a_contract_error_and_isolates_the_logical_session() {
    let (mut session, cancels) = open(false).await;
    let frame = Datagram {
        address: support::destination(),
        association: AssociationId(3),
        payload: vec![0; 2].into(),
    };
    let mut tx = PartialSuccess;
    let failure = send(
        Pin::new(&mut tx),
        session.control.as_mut(),
        &frame,
        &session.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(failure.delivery, DeliveryState::Unknown);
    assert_eq!(failure.error.reason, Reason::Internal);
    assert_eq!(cancels.load(Ordering::SeqCst), 1);
}
