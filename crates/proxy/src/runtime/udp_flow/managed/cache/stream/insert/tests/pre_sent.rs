use super::*;
use crate::runtime::udp_flow::managed::connection::{
    managed_tuple_udp_connection_from_flow, ManagedTupleUdpFlowConnection,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use zero_core::Address;

struct Connection(Arc<AtomicBool>, Arc<AtomicUsize>);
#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for Connection {
    fn is_closed(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
    async fn send(&self, _: &Address, _: u16, payload: &[u8]) -> Result<usize, EngineError> {
        assert!(
            !self.is_closed(),
            "closed cached connection must not be reused"
        );
        self.1.fetch_add(1, Ordering::Relaxed);
        Ok(payload.len())
    }
    fn subscribe_responses(&self) -> tokio::sync::broadcast::Receiver<(Address, u16, Vec<u8>)> {
        tokio::sync::broadcast::channel(1).1
    }
    fn closed_message(&self) -> &'static str {
        "test connection closed"
    }
}

#[tokio::test]
async fn closed_cached_connection_is_reestablished_and_live_connection_is_reused() {
    let mut cache = ManagedUdpConnectionCache::new();
    let mut tasks = tokio::task::JoinSet::new();
    let target = Address::Ipv4([127, 0, 0, 1]);
    let packet = UdpPacketRef {
        target: &target,
        port: 53,
        payload: b"query",
    };
    let old_closed = Arc::new(AtomicBool::new(false));
    let sends = Arc::new(AtomicUsize::new(0));
    let established = Arc::new(AtomicUsize::new(0));
    for (index, closed) in [
        old_closed.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ]
    .into_iter()
    .enumerate()
    {
        if index == 1 {
            old_closed.store(true, Ordering::Relaxed);
        }
        let establish = async {
            established.fetch_add(1, Ordering::Relaxed);
            Ok::<SharedManagedUdpConnection, EngineError>(managed_tuple_udp_connection_from_flow(
                Connection(closed, sends.clone()),
            ))
        };
        assert_eq!(
            cache
                .send_or_insert_pre_sent_key("scope", &mut tasks, 7, packet, establish)
                .await
                .unwrap(),
            5
        );
    }
    assert_eq!(established.load(Ordering::Relaxed), 2);
    assert_eq!(sends.load(Ordering::Relaxed), 1);
}
