use super::*;
use crate::{
    mux::{MuxResponseBacklogPolicy, VlessInboundMuxServer},
    reverse::{worker::WorkerStatus, MONITOR_INTERVAL},
};
use std::sync::{Arc, Mutex, Weak};
use zero_traits::AsyncSocket;

/// Protocol-selected worker demand. Runtime owns the tasks consuming returned
/// MUX connections; dropping a worker releases its status from this source.
pub struct VlessReverseBridge {
    leaf: VlessOutboundLeaf,
    workers: Mutex<Vec<Weak<WorkerStatus>>>,
    next_attempt: tokio::sync::Mutex<tokio::time::Instant>,
    uuid: [u8; 16],
    flow: Option<&'static str>,
    testseed: [u32; 4],
    backlog: MuxResponseBacklogPolicy,
}
impl VlessOutboundLeaf {
    pub fn into_reverse_bridge(self) -> VlessReverseBridge {
        let (uuid, flow, testseed, backlog) = self.protocol.reverse_parameters();
        VlessReverseBridge {
            leaf: self,
            workers: Mutex::new(Vec::new()),
            next_attempt: tokio::sync::Mutex::new(tokio::time::Instant::now()),
            uuid,
            flow,
            testseed,
            backlog,
        }
    }
}
impl VlessReverseBridge {
    fn needs_worker(&self) -> bool {
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        workers.retain(|worker| worker.upgrade().is_some_and(|worker| worker.load().active));
        crate::reverse::needs_worker(
            workers
                .iter()
                .filter_map(Weak::upgrade)
                .map(|worker| worker.load()),
        )
    }
    pub async fn next_connection<F, Fut>(
        &self,
        open_socket: F,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<(TcpRelayStream, VlessInboundMuxServer), RuntimeError>
    where
        F: Clone + Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TokioSocket, RuntimeError>> + Send + 'static,
    {
        // Serialize demand checks and retain pacing across failed handshakes.
        let mut next = self.next_attempt.lock().await;
        loop {
            tokio::time::sleep_until(*next).await;
            *next = tokio::time::Instant::now() + MONITOR_INTERVAL;
            if self.needs_worker() {
                break;
            }
        }
        let opening = async {
            let mut stream = self
                .leaf
                .transport
                .open_direct(open_socket, sockets)
                .await?;
            crate::reverse::send_request(&mut stream, &self.uuid, self.flow).await?;
            crate::shared::read_response(&mut stream).await?;
            let stream = if crate::flow::is_vision_flow(self.flow) {
                let bypass = stream.transport_bypass_control();
                TcpRelayStream::new(crate::vision::VisionStream::with_testseed(
                    stream,
                    self.uuid,
                    bypass,
                    self.testseed,
                ))
            } else {
                stream
            };
            let status = Arc::new(WorkerStatus::default());
            let server =
                VlessInboundMuxServer::from_master_uuid_with_auth(self.uuid, None, self.backlog)
                    .with_reverse(status.clone());
            self.workers
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(Arc::downgrade(&status));
            Ok((stream, server))
        };
        tokio::time::timeout(std::time::Duration::from_secs(15), opening)
            .await
            .map_err(|_| {
                RuntimeError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Rvs worker handshake timeout",
                ))
            })?
    }
}
