use super::*;
#[tokio::test]
async fn raw_caller_clone_and_last_drop_reenter_outside_all_original_udp_gates() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (_rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let (old, events, weak) = watch(&tx, Weak::new(), Action::CloseOnLastDrop);
    let mut bytes = [0; 8];
    assert!(tx
        .shared
        .socket
        .poll_recv_from_addr(&mut Context::from_waker(&old), &mut bytes)
        .is_pending());
    assert!(events.clones.load(Ordering::SeqCst) > 0);
    assert!(events.observed_live.load(Ordering::SeqCst));
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert_eq!(owner.occupied(), 1);
    drop(old);
    assert_eq!(
        events.last_drop.load(Ordering::SeqCst),
        0,
        "real pending registration retains last caller target"
    );
    let result = tx
        .shared
        .socket
        .poll_recv_from_addr(&mut Context::from_waker(Waker::noop()), &mut bytes);
    assert!(
        matches!(result, Poll::Ready(Err(error)) if error.kind() == std_io::ErrorKind::NotConnected)
    );
    assert!(weak.upgrade().is_none());
    assert_eq!(events.last_drop.load(Ordering::SeqCst), 1);
    let receipt = events.receipt.lock().unwrap().unwrap();
    assert_eq!(control.close().unwrap(), receipt);
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
}
#[test]
fn actual_raw_receive_coop_zero_wake_closes_original_after_native_poll_returns() {
    synchronous_coop(async {
        let mut owner = ControlledDatagramOwner::new(bound(1));
        let (_rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
        let (waker, events, _) = watch(&tx, Weak::new(), Action::CloseOnWake);
        let mut bytes = [0; 8];
        assert!(tx
            .shared
            .socket
            .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
            .is_pending());
        assert_eq!(owner.take_report().confirmed_closed, 0);
        exhaust();
        assert!(tx
            .shared
            .socket
            .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
            .is_pending());
        assert!(events.wakes.load(Ordering::SeqCst) > 0);
        assert!(events.observed_live.load(Ordering::SeqCst));
        let receipt = events.receipt.lock().unwrap().unwrap();
        assert_eq!(receipt.reservation(), control.reservation());
        assert_eq!(control.close().unwrap(), receipt);
        assert_eq!(owner.take_report().confirmed_closed, 1);
        assert_eq!(owner.occupied(), 0);

        drop(waker);
        assert_eq!(events.owner_gone_drops.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn listener_native_send_coop_zero_preserves_pending_admission_and_real_close_receipt() {
    synchronous_coop(async {
        let mut owner = ControlledDatagramOwner::new(bound(1));
        let (_rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
        let (waker, events, _) = watch(&tx, Weak::new(), Action::CloseOnWake);
        let mut send = Box::pin(tx.send_to_addr(b"pending", tx.local_addr().unwrap()));
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert_eq!(tx.shared.sends.available_permits(), 0);
        assert_eq!(owner.take_report().confirmed_closed, 0);
        exhaust();
        assert!(send
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert!(events.wakes.load(Ordering::SeqCst) > 0);
        let receipt = events.receipt.lock().unwrap().unwrap();
        assert_eq!(receipt, control.close().unwrap());
        drop(send);
        assert_eq!(tx.shared.sends.available_permits(), 1);
        assert_eq!(owner.take_report().confirmed_closed, 1);

        drop(waker);
        assert_eq!(events.owner_gone_drops.load(Ordering::SeqCst), 0);
    });
}

#[tokio::test]
async fn raw_receive_cancel_reenters_same_actual_control_without_native_close_ack() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (_rx, tx, control) = owner.bind_listener(address(), bound(1)).unwrap();
    let control = Arc::new(control);
    let (waker, events, _) = watch(&tx, Arc::downgrade(&control), Action::RevokeOnWake);
    let mut bytes = [0; 8];
    assert!(tx
        .shared
        .socket
        .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
        .is_pending());
    control.revoke();
    assert!(events.wakes.load(Ordering::SeqCst) > 0);
    assert!(events.receipt.lock().unwrap().is_none());
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert_eq!(owner.occupied(), 1);
    assert!(
        matches!(tx.shared.socket.poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes), Poll::Ready(Err(error)) if error.kind() == std_io::ErrorKind::NotConnected)
    );
    let mut control = Arc::try_unwrap(control).ok().unwrap();
    control.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
#[tokio::test]
async fn completed_actual_receive_releases_last_caller_target_without_dropping_original() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (_rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let local = tx.local_addr().unwrap();
    let (waker, events, weak) = watch(&tx, Weak::new(), Action::Check);
    let mut bytes = [0; 8];
    assert!(tx
        .shared
        .socket
        .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
        .is_pending());
    peer.send_to(b"real", local).await.unwrap();
    let result = loop {
        match tx
            .shared
            .socket
            .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
        {
            Poll::Ready(result) => break result.unwrap(),
            Poll::Pending => tokio::task::yield_now().await,
        }
    };
    assert_eq!(result, (4, peer.local_addr().unwrap()));
    assert_eq!(&bytes[..4], b"real");
    drop(waker);
    assert!(
        weak.upgrade().is_none(),
        "completed native poll must not retain a caller task"
    );
    assert_eq!(events.last_drop.load(Ordering::SeqCst), 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert_eq!(owner.occupied(), 1);
    control.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
