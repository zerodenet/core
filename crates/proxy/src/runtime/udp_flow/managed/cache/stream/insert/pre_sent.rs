use std::future::Future;

use zero_engine::EngineError;

use super::super::model::ManagedUdpConnectionCache;
use crate::runtime::udp_flow::managed::connection::SharedManagedUdpConnection;
use crate::runtime::udp_flow::packet_path::{ChainTask, UdpPacketRef};

impl ManagedUdpConnectionCache {
    async fn send_or_insert_pre_sent<Fut>(
        &mut self,
        key: super::super::super::key::ManagedUdpConnectionCacheKey,
        chain_tasks: &mut tokio::task::JoinSet<ChainTask>,
        session_id: u64,
        packet: UdpPacketRef<'_>,
        establish: Fut,
    ) -> Result<usize, EngineError>
    where
        Fut: Future<Output = Result<SharedManagedUdpConnection, EngineError>>,
    {
        let sent = packet.payload.len();
        if let Some(connection) = self
            .entries
            .get(&key)
            .filter(|connection| !connection.is_closed())
        {
            connection.spawn_response_bridge(chain_tasks, session_id);
            return connection
                .send(packet.target, packet.port, packet.payload)
                .await;
        }

        // Reclaim retired handles on a cache miss, not on every live UDP packet.
        self.entries.retain(|_, connection| !connection.is_closed());
        let connection = establish.await?;
        connection.spawn_response_bridge(chain_tasks, session_id);
        self.entries.insert(key, connection);
        Ok(sent)
    }

    pub(crate) async fn send_or_insert_pre_sent_key<Fut>(
        &mut self,
        key: impl Into<String>,
        chain_tasks: &mut tokio::task::JoinSet<ChainTask>,
        session_id: u64,
        packet: UdpPacketRef<'_>,
        establish: Fut,
    ) -> Result<usize, EngineError>
    where
        Fut: Future<Output = Result<SharedManagedUdpConnection, EngineError>>,
    {
        self.send_or_insert_pre_sent(
            super::super::super::key::ManagedUdpConnectionCacheKey::new(key),
            chain_tasks,
            session_id,
            packet,
            establish,
        )
        .await
    }
}

#[cfg(test)]
#[path = "tests/pre_sent.rs"]
mod tests;
