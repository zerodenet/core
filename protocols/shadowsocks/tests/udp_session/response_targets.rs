//! Pure response FIFO matching, not native provider or close qualification.
use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn numeric_wire_domain_matches_only_the_same_authenticated_ip_and_port() {
    for (wire, ip, foreign) in [
        (
            "127.0.0.1",
            Address::Ipv4([127, 0, 0, 1]),
            Address::Ipv4([127, 0, 0, 2]),
        ),
        (
            "[::1]",
            Address::Ipv6(std::net::Ipv6Addr::LOCALHOST.octets()),
            Address::Ipv6(std::net::Ipv6Addr::UNSPECIFIED.octets()),
        ),
    ] {
        let responses = Arc::new(Responses::default());
        let pending = responses
            .register(&Address::Domain(wire.into()), 5353)
            .unwrap();
        tokio::pin!(pending);
        assert!(responses.deliver_pending((foreign, 5353, vec![1])).is_err());
        assert!(responses
            .deliver_pending((ip.clone(), 5354, vec![2]))
            .is_err());
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        responses
            .deliver_pending((ip.clone(), 5353, vec![3]))
            .unwrap();
        assert_eq!(pending.await.unwrap(), (ip, 5353, vec![3]));
    }
}

#[tokio::test]
async fn hostname_waiter_requires_its_original_hostname_and_keeps_fifo() {
    let responses = Arc::new(Responses::default());
    let target = Address::Domain("original.test".into());
    let first = responses.register(&target, 53).unwrap();
    let second = responses.register(&target, 53).unwrap();
    assert!(responses
        .deliver_pending((Address::Ipv4([203, 0, 113, 1]), 53, vec![0]))
        .is_err());
    assert!(responses
        .deliver_pending((Address::Domain("foreign.test".into()), 53, vec![0]))
        .is_err());
    responses
        .deliver_pending((target.clone(), 53, vec![1]))
        .unwrap();
    responses
        .deliver_pending((target.clone(), 53, vec![2]))
        .unwrap();
    assert_eq!(first.await.unwrap(), (target.clone(), 53, vec![1]));
    assert_eq!(second.await.unwrap(), (target, 53, vec![2]));
}
