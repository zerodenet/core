use super::*;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    time::{timeout, Instant},
};

type Closed = tokio::task::JoinHandle<Result<(), h2::Error>>;
async fn server(reply: bool) -> (String, tokio::sync::oneshot::Receiver<()>, Closed) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("h2c://{}/dns-query", listener.local_addr().unwrap());
    let (entered, pending) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await?;
        let (request, mut respond) = connection.accept().await.unwrap()?;
        assert_eq!(request.uri().path(), "/dns-query");
        let mut query = vec![];
        let mut body = request.into_body();
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            query.extend_from_slice(&chunk);
            body.flow_control().release_capacity(chunk.len())?;
        }
        assert_eq!(
            crate::message::parse_question(&query).unwrap().query_type,
            crate::message::TYPE_HTTPS
        );
        if reply {
            // A real successful HTTPS response without ECH material. The
            // material parser has separate wire tests; this checks its pool.
            let response = crate::message::build_address_response(&query, &[], 120);
            let mut send = respond.send_response(
                http::Response::builder().status(200).body(()).unwrap(),
                false,
            )?;
            send.send_data(Bytes::from(response), true)?;
        }
        entered.send(()).unwrap();
        // In the pending case retain the responder and drive the actual peer
        // connection until cancellation closes it, without producing a reply.
        let _held = respond;
        match connection.accept().await {
            None => Ok(()),
            Some(Err(source)) => Err(source),
            Some(Ok(_)) => panic!("unexpected second ECH request"),
        }
    });
    (endpoint, pending, task)
}
async fn assert_peer_closed(task: Closed) {
    match timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
    {
        Ok(()) => {}
        // An explicit hard close can be EOF or a native reset with unread H2
        // frames. Keep that exact error and reject every other failure.
        Err(source) => assert_eq!(
            source.get_io().map(io::Error::kind),
            Some(io::ErrorKind::ConnectionReset),
            "unexpected ECH peer close: {source:?}"
        ),
    }
}
fn until() -> Instant {
    Instant::now() + Duration::from_secs(2)
}

#[tokio::test]
async fn temporary_ech_doh_pool_is_registered_and_joined_before_success_returns() {
    let (endpoint, entered, peer) = server(true).await;
    let dns = DnsSystem::build(None).unwrap();
    let answer = timeout(
        Duration::from_secs(2),
        dns.query_ech_config("ech.owner.test", &endpoint),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(answer.config_list, None);
    entered.await.unwrap();
    let owners = dns.transport_owners.doh_snapshot();
    assert_eq!(
        owners.len(),
        1,
        "the temporary resolver registered its actual owner"
    );
    assert!(
        owners[0].drained(),
        "success confirms the actual driver join"
    );
    assert_peer_closed(peer).await;
    dns.shutdown_doh_pools(until()).await.unwrap();
}

#[tokio::test]
async fn cancelling_a_real_pending_ech_request_preserves_its_owner_for_host_join() {
    let (endpoint, entered, peer) = server(false).await;
    let dns = Arc::new(DnsSystem::build(None).unwrap());
    let query = tokio::spawn({
        let dns = dns.clone();
        async move {
            dns.query_ech_config("pending.ech.owner.test", &endpoint)
                .await
        }
    });
    timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    query.abort();
    assert!(query.await.unwrap_err().is_cancelled());
    let owners = dns.transport_owners.doh_snapshot();
    assert_eq!(
        owners.len(),
        1,
        "caller drop retains the actual owner in DNS"
    );
    dns.shutdown_doh_pools(until()).await.unwrap();
    assert!(owners[0].drained());
    assert_peer_closed(peer).await;
}

#[tokio::test]
async fn confirmed_doh_shutdown_prevents_a_late_temporary_ech_pool_from_opening() {
    let dns = DnsSystem::build(None).unwrap();
    dns.shutdown_doh_pools(until()).await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("h2c://{}/dns-query", listener.local_addr().unwrap());
    let source = dns
        .query_ech_config("late.ech.owner.test", &endpoint)
        .await
        .unwrap_err();
    assert_eq!(source.kind(), io::ErrorKind::NotConnected);
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
