//! Original ordinary wire exchange; all test callers preserve native custody.
use super::*;
#[path = "echo/progress.rs"]
mod progress;
use progress::{AtDeadline, Counted, Progress};
use std::sync::{atomic::Ordering, Arc};
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
    let progress = Arc::new(Progress::default());
    let _deadline = AtDeadline {
        client,
        progress: progress.clone(),
        port,
    };
    let payload: Vec<u8> = (0..262144).map(|index| (index % 251) as u8).collect();
    let expected = payload.clone();
    let suffix: Vec<u8> = (0..tail).map(|index| (index % 251) as u8).collect();
    let mut expected_response = payload.clone();
    expected_response.extend_from_slice(&suffix);
    let target_progress = progress.clone();
    let peer = tokio::spawn(async move {
        target_progress.target.store(1, Ordering::Release);
        let (io, _) = target.accept().await.unwrap();
        let mut io = Counted {
            io,
            progress: target_progress.clone(),
            target: true,
        };
        target_progress.target.store(2, Ordering::Release);
        let mut body = Vec::new();
        io.read_to_end(&mut body).await.unwrap();
        target_progress.target.store(3, Ordering::Release);
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
        target_progress.target.store(4, Ordering::Release);
        io.write_all(&body).await.unwrap();
        target_progress.target.store(5, Ordering::Release);
        io.shutdown().await.unwrap();
        target_progress.target.store(6, Ordering::Release);
    });
    progress.client.store(1, Ordering::Release);
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
    progress.client.store(2, Ordering::Release);
    let mut io = Counted {
        io,
        progress: progress.clone(),
        target: false,
    };
    // No application read while outgoing SETTINGS/WINDOW_UPDATE and writes
    // progress; then preserve a real write half-close and full response body.
    progress.client.store(3, Ordering::Release);
    io.write_all(&payload).await.unwrap();
    progress.client.store(4, Ordering::Release);
    io.flush().await.unwrap();
    progress.client.store(5, Ordering::Release);
    let original = inspect();
    progress.client.store(6, Ordering::Release);
    io.shutdown().await.unwrap();
    progress.client.store(7, Ordering::Release);
    let mut received = Vec::new();
    progress.client.store(8, Ordering::Release);
    let reverse = io.read_to_end(&mut received).await;
    progress.client.store(9, Ordering::Release);
    eprintln!("actual VMess reverse read={reverse:?} bytes={} active boundaries(id,port,up,down,inRx,inTx,outRx,outTx)={:?}",received.len(),client.proxy.engine().active_sessions().iter().map(|flow|(flow.id,flow.port,flow.bytes_up,flow.bytes_down,flow.inbound_rx_bytes,flow.inbound_tx_bytes,flow.outbound_rx_bytes,flow.outbound_tx_bytes)).collect::<Vec<_>>());
    reverse.unwrap();
    // Consume the real target task before diagnosing a truncated reverse body:
    // upload validation and target write/shutdown cannot be hidden by its panic.
    peer.await.unwrap();
    progress.client.store(10, Ordering::Release);
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
    let result = timeout(Duration::from_secs(3), async {
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
    .await;
    assert!(
        result.is_ok(),
        "same actual original failed retirement: {:?}",
        retirement_facts(owner)
    );
    let closed = original.closed().unwrap();
    assert!(original.matches_closed(&closed) && closed.had_native_acquisition());
}

pub(crate) fn retirement_facts(owner: &CarrierOwnerRegistration) -> Vec<String> {
    owner.observe().map_or_else(Vec::new, |actual| actual.slots().iter().map(|slot| {
        let witness = |actual: &zero_platform_tokio::contract::scope::TaskJoinWitness| {
            let joined = actual.try_joined();
            (actual.task_id(), actual.is_joined(), joined.as_ref().map(|joined| joined.registration_released()),
                joined.as_ref().map(|joined| joined.failure_accepted()), joined.as_ref().map(|joined| joined.result().is_ok()))
        };
        format!("instance={:?} ordinal={} opening={} spawning={} physicalViewLive={} driver={:?} auxiliary={:?} nativeClosed={:?} nativePending={:?} nativeFault={:?} acquisitionUnknown={}",
            owner.instance(), slot.ordinal(), slot.is_opening(), slot.is_spawning(), slot.physical_view_live(),
            slot.driver().map(|(_, task)| witness(task)), slot.auxiliary().map(witness),
            slot.native().map(|native| native.is_closed()), slot.native().map(|native| native.operation_pending()),
            slot.native().and_then(|native| native.first_fault()).map(|source| source.kind()), slot.acquisition_unknown().is_some())
    }).collect())
}
