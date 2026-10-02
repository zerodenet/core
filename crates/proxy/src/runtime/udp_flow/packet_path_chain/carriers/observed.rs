//! Observe a hop's encoded datagram boundary, below its codec.
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;
use std::{net::SocketAddr, sync::Arc};
use zero_core::Address;
use zero_engine::EngineError;
use zero_traits::IoObserver;

pub(crate) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    observer: Option<Arc<dyn IoObserver>>,
) -> Arc<dyn PacketPathCarrier> {
    match observer {
        Some(observer) => {
            observer.datagram_boundary();
            Arc::new(Observed { path, observer })
        }
        None => path,
    }
}
struct Observed {
    path: Arc<dyn PacketPathCarrier>,
    observer: Arc<dyn IoObserver>,
}
#[async_trait::async_trait]
impl PacketPathCarrier for Observed {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let result = self.path.send_to(target, port, payload).await;
        match &result {
            Ok(()) => self.observer.sent_datagram(payload.len()),
            Err(_) => self.observer.error(),
        }
        result
    }
    async fn recv_from(&self, output: &mut [u8]) -> Result<usize, EngineError> {
        let result = self.path.recv_from(output).await;
        match &result {
            Ok(size) => self.observer.received_datagram(*size),
            Err(_) => self.observer.error(),
        }
        result
    }
    async fn recv_from_with_source(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        let result = self.path.recv_from_with_source(output).await;
        match &result {
            Ok((size, _)) => self.observer.received_datagram(*size),
            Err(_) => self.observer.error(),
        }
        result
    }
}
