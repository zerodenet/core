//! The user census proves handoff exit only; native closure has separate providers.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::{
    component::{AssociationId, SessionId},
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::contract::{
    datagram::{erase_session, DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
    lifecycle::ChannelControl,
    SendFailure, SendReceipt,
};

struct Tx;
struct Rx(Option<Datagram>);
struct Control;
impl DatagramTx for Tx {
    fn poll_send(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &Datagram,
    ) -> Poll<Result<SendReceipt, SendFailure>> {
        Poll::Ready(Ok(SendReceipt::accepted(frame.payload.len())))
    }
}
impl DatagramRx for Rx {
    fn poll_recv(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Result<Option<Datagram>, ChannelError>> {
        Poll::Ready(Ok(self.get_mut().0.take()))
    }
}
impl ChannelControl for Control {
    fn cancel(&self, _: CloseReason) {}
    async fn wait_closed(&mut self, _: Instant) -> Result<CloseReport, ChannelError> {
        Ok(CloseReport::default())
    }
}

#[tokio::test]
async fn all_datagram_parts_retain_one_user_and_revocation_is_not_a_native_close_receipt() {
    let users = Arc::new(Users::new(ComponentId(7)));
    users.bind(true);
    let lease = users.enter(Some(SessionId(9))).unwrap().unwrap();
    let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), 80);
    let frame = || Datagram {
        address: destination.clone(),
        association: AssociationId(1),
        payload: vec![].into(),
    };
    let mut session = lease.datagram(erase_session(DatagramSession {
        tx: Tx,
        rx: Rx(Some(frame())),
        control: Control,
        limits: DatagramLimits {
            max_payload: 128,
            targets: DatagramTargets::Fixed(destination.clone()),
            queued_bytes: 128,
        },
    }));
    let received = std::future::poll_fn(|cx| session.rx.as_mut().poll_recv(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(received.payload.is_empty(), "an empty datagram is not EOF");
    let send = frame();
    assert_eq!(
        std::future::poll_fn(|cx| session.tx.as_mut().poll_send(cx, &send))
            .await
            .unwrap()
            .accepted_bytes,
        0
    );
    users.revoke();
    let error = std::future::poll_fn(|cx| session.rx.as_mut().poll_recv(cx))
        .await
        .unwrap_err();
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(error.component, Some(ComponentId(7)));
    assert_eq!(error.session, Some(SessionId(9)));
    let send = std::future::poll_fn(|cx| session.tx.as_mut().poll_send(cx, &send))
        .await
        .unwrap_err();
    assert_eq!(send.delivery, DeliveryState::Unknown);
    drop(session.tx);
    drop(session.rx);
    assert!(!users.wait_idle(Instant::now()).await.is_complete());
    assert!(session
        .control
        .wait_closed(Instant::now())
        .await
        .unwrap()
        .is_complete());
    assert!(
        !users.wait_idle(Instant::now()).await.is_complete(),
        "the transferred control is still held"
    );
    drop(session.control);
    let report = users.wait_idle(Instant::now()).await;
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "ending user custody never fabricates native I/O proof"
    );
    assert!(
        users.enter(None).is_err(),
        "a retired generation never admits a late open"
    );
}

#[tokio::test]
async fn cached_retired_role_handles_do_not_keep_actual_protocol_roles_or_recipes_alive() {
    let catalog = crate::component::catalog::ComponentCatalog::default();
    let original = Arc::new(zero_config::RuntimeConfig::parse(r#"{"outbounds":[{"tag":"removed","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap());
    let deadline = || Instant::now() + std::time::Duration::from_secs(3);
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let slot = catalog
        .current(&original)
        .unwrap()
        .configured("removed")
        .unwrap();
    let cached = catalog
        .drive_supervised(slot.stream(deadline()))
        .await
        .unwrap();
    let weak_slot = Arc::downgrade(&slot);
    let weak_role = {
        let owner = slot
            .live
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .owner
            .lock()
            .await;
        Arc::downgrade(owner.instance.roles.stream.as_ref().unwrap())
    };
    let replacement = Arc::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
    );
    catalog
        .start(replacement.clone(), deadline())
        .await
        .unwrap();
    catalog.commit(&replacement, || Ok(())).unwrap();
    drop(slot);
    assert!(weak_slot.upgrade().is_none());
    assert!(
        weak_role.upgrade().is_none(),
        "cached public role wrappers retain identity, not the protocol's private resources"
    );
    drop(cached);
    catalog.shutdown(deadline()).await.unwrap();
}

#[test]
fn same_actual_idle_user_root_retains_nested_catalog_and_source_pauses_without_admission_gap() {
    let users = Arc::new(Users::new(ComponentId(72)));
    users.bind(true);
    let admitted = users.enter(Some(SessionId(91))).unwrap().unwrap();
    assert!(users.try_pause().is_err());
    drop(admitted);
    let source = users.try_pause().unwrap();
    let catalog = users.try_pause().unwrap();
    assert!(users.enter(Some(SessionId(92))).is_err());
    drop(source);
    assert!(
        users.enter(Some(SessionId(93))).is_err(),
        "same real catalog guard still holds admission"
    );
    drop(catalog);
    let actual = users.enter(Some(SessionId(94))).unwrap().unwrap();
    drop(actual);
    assert_eq!(users.active.load(Ordering::Acquire), 0);
}
