use super::*;
use zero_core::Address;

fn packet(id: u32, marker: u8) -> Hysteria2UdpPacket {
    Hysteria2UdpPacket::new(id, 0, Address::Domain("echo.test".into()), 53, vec![marker])
}
#[test]
fn full_or_unknown_udp_session_does_not_block_or_deliver_to_another_session() {
    let mut state = State::default();
    let (a, mut a_rx) = mpsc::channel(2);
    let (b, mut b_rx) = mpsc::channel(2);
    state.sessions.insert(1, a);
    state.sessions.insert(2, b);
    for _ in 0..10 {
        state.deliver(packet(1, 1));
    }
    state.deliver(packet(999, 9));
    state.deliver(packet(2, 2));
    assert_eq!(a_rx.len(), 2);
    assert_eq!(a_rx.try_recv().unwrap().payload(), &[1]);
    assert_eq!(b_rx.try_recv().unwrap().payload(), &[2]);
    assert!(b_rx.try_recv().is_err());
    assert_eq!(state.sessions.len(), 2);
}

#[tokio::test]
async fn session_ids_do_not_wrap_or_reuse_after_close_and_connection_drop_wakes_receivers() {
    let (client, _server) = crate::transport::test_fixtures::pair().await;
    let dispatcher = Dispatcher::new(client);
    dispatcher.state.lock().unwrap().next_id = u32::MAX as u64;
    let mut last = dispatcher.register().unwrap();
    assert_eq!(last.id, u32::MAX);
    assert!(dispatcher.register().is_err());
    drop(dispatcher);
    assert!(last.receiver.recv().await.is_none());
    assert!(last.state.lock().unwrap().sessions.is_empty());
}
