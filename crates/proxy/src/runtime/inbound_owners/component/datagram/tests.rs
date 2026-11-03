//! The actual declared UDP original is distinct from returned views and port probes.
use super::*;
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    task::Poll,
    time::Duration,
};

#[tokio::test]
async fn paired_udp_provider_uses_actual_tcp_endpoint_and_consumes_original_close_receipt() {
    let one = NonZeroUsize::new(1).unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = tcp.local_addr().unwrap();
    let declared = Destination::new(zero_core::Address::Domain("localhost".into()), 0);
    let provider = DatagramResource::new(
        zero_core::component::ComponentId(7301),
        &ResourceDeclaration::DatagramListener {
            destination: declared.clone(),
            max_bind_attempts: one,
            max_pending_sends: one,
            accepted: Some(DatagramPeerLimits {
                max_tasks_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
                max_resources_per_peer: 0,
                max_peers: one,
                packets_per_peer: one,
            }),
        },
        endpoint,
    )
    .unwrap();
    assert!(provider
        .open(Destination::new(
            zero_core::Address::Ipv4([127, 0, 0, 1]),
            0
        ))
        .is_err());
    let GrantedDatagramListener { mut rx, tx } = provider.open(declared.clone()).unwrap();
    assert_eq!(
        rx.local_addr().unwrap(),
        endpoint,
        "actual chosen TCP endpoint, not a second DNS resolution"
    );
    assert!(provider
        .identity
        .get()
        .unwrap()
        .same_listener(&tx.identity()));
    assert!(
        provider.open(declared).is_err(),
        "one native original per generation"
    );
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut bytes = [0; 8];
    client.send_to(b"data", endpoint).await.unwrap();
    let (size, peer) = rx.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"data");
    assert_eq!(peer, client.local_addr().unwrap());
    {
        let mut waiting = Box::pin(rx.recv_from_addr(&mut bytes));
        poll_fn(|cx| match waiting.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            _ => panic!("the actual original receive must still be pending"),
        })
        .await;
    }
    let report = provider
        .close(Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(
        report.closed_io.len(),
        1,
        "consume original native provider confirmation"
    );
    assert!(
        tx.send_to_addr(b"old", peer).await.is_err(),
        "transferred native view is retired"
    );
    assert!(rx.recv_from_addr(&mut bytes).await.is_err());
}
