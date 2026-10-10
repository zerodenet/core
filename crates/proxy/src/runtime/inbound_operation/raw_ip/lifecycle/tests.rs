use super::*;

#[tokio::test(start_paused = true)]
async fn peer_changes_wake_a_parked_listener_without_packet_io() {
    let state = |revision| {
        Arc::new(EndpointPeerState {
            revision,
            devices: vec![],
            initial_endpoints: vec![],
            carriers: vec![],
        })
    };
    let (sender, receiver) = watch::channel(state(1));
    let mut receiver = Some(receiver);
    let timer = crate::runtime::raw_ip::timer::wait(None);
    tokio::pin!(timer);
    sender.send_replace(state(2));
    tokio::select! {
        _ = &mut timer => panic!("parked timer woke"),
        changed = peers_changed(&mut receiver) => assert!(changed.is_ok()),
    }
    assert_eq!(receiver.as_ref().unwrap().borrow().revision, 2);
    drop(sender);
    assert!(peers_changed(&mut receiver).await.is_err());
}
