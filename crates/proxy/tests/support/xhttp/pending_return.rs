//! A real UDP caller returns while a TCP target is still awaiting native I/O.
use super::super::interop::{socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_udp_echo_count};
use super::super::{free_udp_port, wait_for};
use tokio::sync::watch;
use zero_proxy::RunningProxy;

pub(crate) async fn check(socks: u16, owner: &RunningProxy) {
    let payload = vec![0x36; 131_073];
    let expected = payload.clone();
    let (release, returning) = watch::channel(false);
    let (port, target, pending) = super::pending_echo::start(payload.len(), returning).await;
    let tcp = tokio::spawn(async move {
        let received = socks5_tcp_echo_once(socks, port, &payload).await.unwrap();
        assert_eq!(received, payload);
        received
    });
    pending.await.unwrap();
    let udp_port = free_udp_port();
    let echo = spawn_udp_echo_count(udp_port, 1).await;
    let packets: &[&[u8]] = &[b"actual UDP return beside a pending TCP original"];
    assert_eq!(
        socks5_udp_echo_sequence(socks, udp_port, packets).await,
        packets
    );
    echo.await.unwrap();
    wait_for("the actual UDP caller returned", || {
        owner
            .completed_sessions()
            .iter()
            .any(|session| session.network == zero_core::Network::Udp && session.port == udp_port)
    })
    .await;
    assert!(
        !tcp.is_finished(),
        "UDP return cannot retire the waiting TCP"
    );
    release.send(true).unwrap();
    assert_eq!(tcp.await.unwrap(), expected);
    target.await.unwrap();
}
