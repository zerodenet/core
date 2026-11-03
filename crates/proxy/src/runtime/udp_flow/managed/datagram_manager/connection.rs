//! Actual pre-sent connection ownership, with typed protocol reconstruction.
use super::ManagedDatagramResumeConnector;
use crate::{
    protocol_registry::UdpRuntimeServices,
    runtime::{
        path::OutboundEndpoint,
        udp_flow::{
            managed::connection::{
                managed_tuple_udp_connection_from_flow, ManagedDatagramUdpConnection,
                SharedManagedDatagramUdpConnection, SharedManagedUdpConnection,
            },
            packet_path::{ChainTask, UdpPacketRef},
        },
    },
};
use std::sync::Arc;
use tokio::{sync::Mutex, task::JoinSet};
use zero_core::Address;
use zero_engine::EngineError;

struct Connection<T> {
    // This is the protocol's concrete wire plan, never Any or a saved config /
    // engine / registry. Reopen receives the current narrow services at send.
    plan: T,
    endpoint: OutboundEndpoint,
    current: Mutex<Option<SharedManagedUdpConnection>>,
}
#[async_trait::async_trait]
impl<T: ManagedDatagramResumeConnector> ManagedDatagramUdpConnection for Connection<T> {
    fn is_closed(&self) -> bool {
        match self.current.try_lock() {
            Ok(current) => current
                .as_ref()
                .is_none_or(|connection| connection.is_closed()),
            // A suspended open/send is active, never an idle eviction target.
            Err(_) => false,
        }
    }
    async fn send_datagram(
        &self,
        tasks: &mut JoinSet<ChainTask>,
        services: Option<UdpRuntimeServices>,
        session_id: u64,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        let mut current = self.current.lock().await;
        if let Some(connection) = current
            .as_ref()
            .filter(|connection| !connection.is_closed())
        {
            connection.spawn_response_bridge(tasks, session_id);
            return connection.send(target, port, payload).await;
        }
        // Return the retired logical view before attempting another open. This
        // is not native close evidence; the protocol's original providers and
        // host admission fences still decide whether new I/O may be acquired.
        drop(current.take());
        let services = services.ok_or_else(|| {
            EngineError::Io(std::io::Error::other(
                "expected proxy context for managed datagram flow",
            ))
        })?;
        let flow = self
            .plan
            .clone()
            .open_connection(
                services.packet_path_execution(),
                self.endpoint.clone(),
                UdpPacketRef {
                    target,
                    port,
                    payload,
                },
            )
            .await?;
        let connection = managed_tuple_udp_connection_from_flow(flow);
        connection.spawn_response_bridge(tasks, session_id);
        *current = Some(connection);
        // Establishment submitted this exact packet. Never resend it or charge
        // a second upload. Closure here is logical, not a native close receipt.
        Ok(payload.len())
    }
}
pub(crate) fn connection_from_plan<T: ManagedDatagramResumeConnector>(
    plan: T,
    endpoint: OutboundEndpoint,
) -> SharedManagedDatagramUdpConnection {
    Arc::new(Connection {
        plan,
        endpoint,
        current: Mutex::new(None),
    })
}
