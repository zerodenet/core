use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

struct Echo {
    target: Address,
    port: u16,
    responses: broadcast::Sender<(Address, u16, Vec<u8>)>,
}
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for Echo {
    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        assert_eq!((target, port), (&self.target, self.port));
        self.responses
            .send((target.clone(), port, payload.to_vec()))
            .unwrap();
        Ok(payload.len())
    }
    fn subscribe_responses(&self) -> broadcast::Receiver<(Address, u16, Vec<u8>)> {
        self.responses.subscribe()
    }
    fn closed_message(&self) -> &'static str {
        "echo closed"
    }
}
fn carrier(idle: Duration) -> (Arc<TupleCarrier>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let opened = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let (create, close) = (opened.clone(), closed.clone());
    let open: Open = Arc::new(|target, port| {
        Box::pin(async move {
            tokio::task::yield_now().await;
            Ok(Box::new(Echo {
                target,
                port,
                responses: broadcast::channel(16).0,
            }) as Box<dyn ManagedTupleUdpFlowConnection>)
        })
    });
    (
        Arc::new(TupleCarrier::new(
            open,
            idle,
            Arc::new(move |opening| {
                if opening.is_none() {
                    create.fetch_add(1, Ordering::SeqCst);
                } else {
                    close.fetch_add(1, Ordering::SeqCst);
                }
            }),
        )),
        opened,
        closed,
    )
}

#[tokio::test]
async fn concurrent_packets_reuse_target_flow_without_losing_first_reply() {
    let (carrier, opened, closed) = carrier(Duration::from_secs(60));
    let mut jobs = tokio::task::JoinSet::new();
    for value in 0..12u8 {
        let carrier = carrier.clone();
        jobs.spawn(async move {
            carrier
                .send_to(
                    &Address::Domain("destination.test".into()),
                    1000 + u16::from(value % 3),
                    &[value],
                )
                .await
                .unwrap();
        });
    }
    while let Some(result) = jobs.join_next().await {
        result.unwrap();
    }
    let mut received = Vec::new();
    for _ in 0..12 {
        let mut packet = [0; 8];
        let count = tokio::time::timeout(Duration::from_secs(1), carrier.recv_from(&mut packet))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
        received.push(packet[0]);
    }
    received.sort();
    assert_eq!(received, (0..12).collect::<Vec<_>>());
    assert_eq!(opened.load(Ordering::SeqCst), 3);
    drop(carrier);
    assert_eq!(closed.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn idle_tuple_flows_are_reclaimed_and_recreated_on_later_send() {
    let (carrier, opened, closed) = carrier(Duration::from_secs(2));
    let target = Address::Domain("destination.test".into());
    carrier.send_to(&target, 53, b"first").await.unwrap();
    let mut packet = [0; 16];
    carrier.recv_from(&mut packet).await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert_eq!(closed.load(Ordering::SeqCst), 1);
    carrier.send_to(&target, 53, b"second").await.unwrap();
    assert_eq!(carrier.recv_from(&mut packet).await.unwrap(), 6);
    assert_eq!(opened.load(Ordering::SeqCst), 2);
    drop(carrier);
    assert_eq!(closed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn target_capacity_rejects_new_flow_without_evicting_existing_association() {
    let (carrier, opened, closed) = carrier(Duration::from_secs(60));
    let target = Address::Domain("destination.test".into());
    for port in 1..=64 {
        carrier.send_to(&target, port, b"packet").await.unwrap();
    }
    assert!(carrier.send_to(&target, 65, b"too many").await.is_err());
    carrier.send_to(&target, 1, b"existing").await.unwrap();
    assert_eq!(opened.load(Ordering::SeqCst), 64);
    assert_eq!(closed.load(Ordering::SeqCst), 0);
    drop(carrier);
    assert_eq!(closed.load(Ordering::SeqCst), 64);
}

#[tokio::test]
async fn failed_handshakes_do_not_consume_target_capacity() {
    let open: Open = Arc::new(|_, _| Box::pin(async { Err(failure("handshake failed")) }));
    let carrier = TupleCarrier::new(open, Duration::from_secs(60), Arc::new(|_| {}));
    let target = Address::Domain("destination.test".into());
    for port in 1..=128 {
        let error = carrier.send_to(&target, port, b"packet").await.unwrap_err();
        assert!(error.to_string().contains("handshake failed"));
        assert!(carrier.slots.lock().unwrap().is_empty());
    }
}
