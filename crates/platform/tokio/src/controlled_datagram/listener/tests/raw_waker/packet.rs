use super::*;
#[test]
fn ancillary_sender_real_coop_zero_defers_raw_waker_until_all_original_gates_release() {
    synchronous_coop(async {
        let mut owner = ControlledDatagramOwner::new(bound(1));
        let (rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
        let local = tx.local_addr().unwrap();
        let (waker, events, _) = watch(&tx, Weak::new(), Action::CloseOnWake);
        let (_, mut sender) = crate::PacketSocketReceiver::from_granted(
            crate::contract::component::GrantedDatagramListener { rx, tx: tx.clone() },
            crate::EgressInterfaceControl::default(),
        );
        let mut pending = Box::pin(sender.send_to(b"pending", local));
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert_eq!(owner.take_report().confirmed_closed, 0);
        exhaust();
        assert!(pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        assert!(events.wakes.load(Ordering::SeqCst) > 0);
        assert!(events.observed_live.load(Ordering::SeqCst));
        let receipt = events.receipt.lock().unwrap().unwrap();
        assert_eq!(control.close().unwrap(), receipt);
        drop(pending);
        assert_eq!(tx.shared.sends.available_permits(), 1);
        assert_eq!(owner.take_report().confirmed_closed, 1);

        drop(waker);
        assert_eq!(events.owner_gone_drops.load(Ordering::SeqCst), 0);
    });
}

#[tokio::test]
async fn pending_listener_receive_checks_raw_clone_drop_outside_retirement_gate() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let (waker, events, _) = watch(&tx, Weak::new(), Action::Check);
    let mut bytes = [0; 8];
    let mut pending = Box::pin(rx.recv_from_addr(&mut bytes));
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    assert!(events.clones.load(Ordering::SeqCst) > 0);
    assert!(pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert_eq!(owner.take_report().confirmed_closed, 0);
    control.close().unwrap();
    drop(pending);
    drop(waker);
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
