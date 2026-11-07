//! Original ordinary wire exchange; all test callers preserve native custody.
use super::*;
pub(crate) async fn echo(client: &Running) -> (CarrierOwnerRegistration, CarrierOriginObservation) {
    echo_with(client, || capture(&client.proxy)).await
}
pub(crate) async fn echo_with<T>(client: &Running, inspect: impl FnOnce() -> T) -> T {
    echo_response_with(client, inspect, 0).await
}
pub(crate) async fn echo_response_with<T>(
    client: &Running,
    inspect: impl FnOnce() -> T,
    tail: usize,
) -> T {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = target.local_addr().unwrap().port();
    let payload: Vec<u8> = (0..262144).map(|index| (index % 251) as u8).collect();
    let expected = payload.clone();
    let suffix: Vec<u8> = (0..tail).map(|index| (index % 251) as u8).collect();
    let mut expected_response = payload.clone();
    expected_response.extend_from_slice(&suffix);
    let peer = tokio::spawn(async move {
        let (mut io, _) = target.accept().await.unwrap();
        let mut body = Vec::new();
        io.read_to_end(&mut body).await.unwrap();
        assert!(
            body == expected,
            "target upload bytes={} expected={} first_difference={:?}",
            body.len(),
            expected.len(),
            body.iter()
                .zip(&expected)
                .position(|(left, right)| left != right)
        );
        body.extend_from_slice(&suffix);
        io.write_all(&body).await.unwrap();
        io.shutdown().await.unwrap();
    });
    let mut io = TcpStream::connect(client.address).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 127, 0, 0, 1];
    request.extend_from_slice(&port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut reply = [0; 10];
    io.read_exact(&mut reply).await.unwrap();
    assert_eq!(
        reply[1],
        0,
        "actual flow failures: {:?}",
        client
            .proxy
            .engine()
            .completed_sessions()
            .iter()
            .filter_map(|flow| flow.failure.as_ref())
            .collect::<Vec<_>>()
    );
    // No application read while outgoing SETTINGS/WINDOW_UPDATE and writes
    // progress; then preserve a real write half-close and full response body.
    io.write_all(&payload).await.unwrap();
    io.flush().await.unwrap();
    let original = inspect();
    io.shutdown().await.unwrap();
    let mut received = Vec::new();
    let reverse = io.read_to_end(&mut received).await;
    eprintln!("actual VMess reverse read={reverse:?} bytes={} active boundaries(id,port,up,down,inRx,inTx,outRx,outTx)={:?}",received.len(),client.proxy.engine().active_sessions().iter().map(|flow|(flow.id,flow.port,flow.bytes_up,flow.bytes_down,flow.inbound_rx_bytes,flow.inbound_tx_bytes,flow.outbound_rx_bytes,flow.outbound_tx_bytes)).collect::<Vec<_>>());
    reverse.unwrap();
    // Consume the real target task before diagnosing a truncated reverse body:
    // upload validation and target write/shutdown cannot be hidden by its panic.
    peer.await.unwrap();
    assert!(
        received == expected_response,
        "client reverse bytes={} expected={} first_difference={:?}; actual flow failures: {:?}",
        received.len(),
        expected_response.len(),
        received
            .iter()
            .zip(&expected_response)
            .position(|(left, right)| left != right),
        client
            .proxy
            .engine()
            .completed_sessions()
            .iter()
            .filter_map(|flow| flow.failure.as_ref())
            .collect::<Vec<_>>()
    );
    drop(io);
    original
}
pub(crate) async fn wait_returned(
    owner: &CarrierOwnerRegistration,
    original: &CarrierOriginObservation,
) {
    timeout(Duration::from_secs(3), async {
        loop {
            if original.closed().is_some()
                && owner
                    .observe()
                    .is_some_and(|observed| observed.retained() == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let closed = original.closed().unwrap();
    assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
}
