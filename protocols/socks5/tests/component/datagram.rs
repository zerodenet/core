#[path = "datagram_fixture.rs"]
mod fixture;
use super::fixtures::deadline;
use fixture::{open, server, Opener};
use std::sync::{Arc, Mutex};
use zero_core::{component::AssociationId, packet::Datagram, request::Destination, Address};
use zero_platform_tokio::{
    contract::datagram::{recv, send},
    TokioDatagramSocket,
};
#[tokio::test]
async fn actual_socks5_association_preserves_multiple_wire_targets_zero_length_and_closes_both_carriers(
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (endpoint, server) = server(2, seen.clone(), None, None).await;
    let opener = Arc::new(Opener {
        control: endpoint,
        raw_local: Mutex::new(None),
        calls: Mutex::new(Vec::new()),
    });
    let (mut instance, mut scope, mut channel) = open(opener.clone()).await;
    for (address, payload) in [
        (
            Destination::new(Address::Ipv4([203, 0, 113, 1]), 53),
            b"payload".as_slice(),
        ),
        (
            Destination::new(Address::Domain("wire.example".into()), 443),
            &[],
        ),
    ] {
        let frame = Datagram {
            address: address.clone(),
            association: AssociationId(8),
            payload: payload.to_vec().into(),
        };
        let receipt = send(
            channel.tx.as_mut(),
            channel.control.as_mut(),
            &frame,
            &channel.limits,
        )
        .await
        .unwrap();
        assert_eq!(receipt.accepted_bytes, payload.len());
        let response = tokio::time::timeout_at(deadline(), recv(channel.rx.as_mut()))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.address, address);
        assert_eq!(response.association, AssociationId(8));
        assert_eq!(response.payload.as_ref(), payload);
    }
    assert_eq!(seen.lock().unwrap().len(), 2);
    let report = channel.control.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 2);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    let local = opener.raw_local.lock().unwrap().unwrap();
    TokioDatagramSocket::bind_addr(local)
        .await
        .unwrap()
        .close()
        .unwrap();
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
#[tokio::test]
async fn interrupted_socks5_close_retains_the_control_stream_and_actual_raw_close_progress() {
    let eof = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let (endpoint, server) = server(
        0,
        Arc::new(Mutex::new(Vec::new())),
        Some(eof.clone()),
        Some(release.clone()),
    )
    .await;
    let opener = Arc::new(Opener {
        control: endpoint,
        raw_local: Mutex::new(None),
        calls: Mutex::new(Vec::new()),
    });
    let (mut instance, mut scope, mut channel) = open(opener.clone()).await;
    {
        let close = channel.control.wait_closed(deadline());
        tokio::pin!(close);
        tokio::select! {result=&mut close=>panic!("peer EOF remains explicitly gated: {result:?}"),_=eof.notified()=>{}}
    }
    let local = opener.raw_local.lock().unwrap().unwrap();
    let replacement = TokioDatagramSocket::bind_addr(local).await.unwrap();
    release.notify_one();
    let report = channel.control.wait_closed(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 2);
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    replacement.close().unwrap();
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}

#[tokio::test]
async fn socks5_validates_business_association_and_actual_domain_wire_size_before_submission() {
    let (endpoint, server) = server(0, Arc::new(Mutex::new(Vec::new())), None, None).await;
    let opener = Arc::new(Opener {
        control: endpoint,
        raw_local: Mutex::new(None),
        calls: Mutex::new(Vec::new()),
    });
    let (mut instance, mut scope, mut channel) = open(opener).await;
    let mut frame = Datagram {
        address: Destination::new(Address::Domain("a".repeat(255)), 53),
        association: AssociationId(9),
        payload: Vec::new().into(),
    };
    let failure = send(
        channel.tx.as_mut(),
        channel.control.as_mut(),
        &frame,
        &channel.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(
        failure.error.reason,
        zero_platform_tokio::contract::Reason::StalePlan
    );
    assert_eq!(
        failure.delivery,
        zero_platform_tokio::contract::DeliveryState::NotSubmitted
    );
    frame.association = AssociationId(8);
    // This meets the exposed minimum-header payload limit but the actual
    // domain header is larger. Reject its full encoded wire size atomically.
    frame.payload = vec![0; channel.limits.max_payload].into();
    let failure = send(
        channel.tx.as_mut(),
        channel.control.as_mut(),
        &frame,
        &channel.limits,
    )
    .await
    .unwrap_err();
    assert_eq!(
        failure.error.reason,
        zero_platform_tokio::contract::Reason::Capacity
    );
    assert_eq!(
        failure.delivery,
        zero_platform_tokio::contract::DeliveryState::NotSubmitted
    );
    assert!(channel
        .control
        .wait_closed(deadline())
        .await
        .unwrap()
        .is_complete());
    tokio::time::timeout_at(deadline(), server)
        .await
        .unwrap()
        .unwrap();
    assert!(instance
        .lifecycle
        .shutdown(deadline())
        .await
        .unwrap()
        .is_complete());
    assert!(scope.shutdown(deadline()).await.is_complete());
}
