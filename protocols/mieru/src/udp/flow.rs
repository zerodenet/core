use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;

use zero_core::{Address, Error};
use zero_traits::DatagramCodec;

use super::packet::{decode_udp_flow_packet, encode_udp_flow_packet, MieruInboundUdpPacket};

/// Codec state for Mieru UDP flow datagrams.
///
/// Mieru UDP flow framing is stateless at this layer; stream encryption state is
/// owned by `MieruOutbound`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MieruUdpFlowCodec;

impl MieruUdpFlowCodec {
    pub fn encode_packet(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<Vec<u8>, Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    pub fn decode_packet(&self, data: &[u8]) -> Result<MieruInboundUdpPacket, Error> {
        decode_udp_flow_packet(data)
    }

    #[cfg(feature = "crypto")]
    pub async fn write_response_tokio<W>(
        &self,
        writer: &mut W,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, Error>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        let frame = self.encode_packet(target, port, payload)?;
        let len = frame.len();
        tokio::io::AsyncWriteExt::write_all(writer, &frame)
            .await
            .map_err(|_| Error::Io("failed to write Mieru UDP response"))?;
        tokio::io::AsyncWriteExt::flush(writer)
            .await
            .map_err(|_| Error::Io("failed to flush Mieru UDP response"))?;
        Ok(len)
    }
}

pub(crate) fn udp_flow_codec() -> impl DatagramCodec<Address, Error = Error> {
    MieruUdpFlowCodec
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruUdpFlowResume {
    username: String,
    password: String,
    relay_chain: bool,
}

impl MieruUdpFlowResume {
    pub fn new(username: &str, password: &str, relay_chain: bool) -> Self {
        Self {
            username: username.to_owned(),
            password: password.to_owned(),
            relay_chain,
        }
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    pub fn flow_requires_relay_upstream(&self) -> bool {
        self.relay_chain
    }

    fn leaf_cache_key(&self, server: &str, port: u16) -> MieruUdpLeafKey {
        self.peer_config().leaf_cache_key(server, port)
    }

    fn flow_key(&self, server: &str, port: u16) -> MieruUdpFlowKey {
        if self.relay_chain {
            MieruUdpFlowKey::Relay
        } else {
            MieruUdpFlowKey::Leaf(self.leaf_cache_key(server, port))
        }
    }

    fn cache_key(&self, server: &str, port: u16, session_id: u64) -> MieruUdpCacheKey {
        MieruUdpCacheKey::from_flow_key(self.flow_key(server, port), session_id)
    }

    pub fn flow_cache_key(&self, server: &str, port: u16, session_id: u64) -> String {
        if self.relay_chain {
            return alloc::format!("relay|session:{session_id}");
        }
        let peer = self.peer_config();
        alloc::format!(
            "leaf|{server}:{port}|username:{}|password:{}",
            peer.username,
            peer.password
        )
    }

    pub fn connector_flow(
        &self,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> MieruUdpConnectorFlow {
        MieruUdpConnectorFlow {
            cache_key: self.flow_cache_key(server, port, session_id),
            requires_relay_upstream: self.flow_requires_relay_upstream(),
        }
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        udp_flow_codec()
    }

    fn peer_config(&self) -> MieruUdpPeerConfig<'_> {
        MieruUdpPeerConfig {
            username: &self.username,
            password: &self.password,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MieruUdpConnectorFlow {
    cache_key: alloc::string::String,
    requires_relay_upstream: bool,
}

impl MieruUdpConnectorFlow {
    pub fn with_namespace(mut self, namespace: &str) -> Self {
        self.cache_key = alloc::format!("{namespace}|{}", self.cache_key);
        self
    }
    pub fn into_parts(self) -> (alloc::string::String, bool) {
        (self.cache_key, self.requires_relay_upstream)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MieruUdpFlowConfig<'a> {
    username: &'a str,
    password: &'a str,
}

impl<'a> MieruUdpFlowConfig<'a> {
    pub fn new(username: &'a str, password: &'a str) -> Self {
        Self { username, password }
    }

    pub fn flow_resume(&self, relay_chain: bool) -> MieruUdpFlowResume {
        MieruUdpFlowResume::new(self.username, self.password, relay_chain)
    }
}

pub fn udp_flow_resume_from_config(
    username: &str,
    password: &str,
    relay_chain: bool,
) -> MieruUdpFlowResume {
    MieruUdpFlowConfig::new(username, password).flow_resume(relay_chain)
}

pub fn connector_flow_from_resume(
    resume: &MieruUdpFlowResume,
    server: &str,
    port: u16,
    session_id: u64,
) -> MieruUdpConnectorFlow {
    resume.connector_flow(server, port, session_id)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MieruUdpFlowKey {
    Leaf(MieruUdpLeafKey),
    Relay,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum MieruUdpCacheKey {
    Leaf(MieruUdpLeafKey),
    Relay { session_id: u64 },
}

impl MieruUdpCacheKey {
    fn from_flow_key(flow_key: MieruUdpFlowKey, session_id: u64) -> Self {
        match flow_key {
            MieruUdpFlowKey::Leaf(leaf_key) => Self::Leaf(leaf_key),
            MieruUdpFlowKey::Relay => Self::Relay { session_id },
        }
    }
}

pub struct MieruUdpFlowStore<T> {
    entries: alloc::collections::BTreeMap<MieruUdpCacheKey, T>,
}

impl<T> Default for MieruUdpFlowStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MieruUdpFlowStore<T> {
    pub fn new() -> Self {
        Self {
            entries: alloc::collections::BTreeMap::new(),
        }
    }

    pub fn get(
        &self,
        resume: &MieruUdpFlowResume,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> Option<&T> {
        let key = resume.cache_key(server, port, session_id);
        self.entries.get(&key)
    }

    pub fn insert(
        &mut self,
        resume: &MieruUdpFlowResume,
        server: &str,
        port: u16,
        session_id: u64,
        value: T,
    ) -> Option<T> {
        let key = resume.cache_key(server, port, session_id);
        self.entries.insert(key, value)
    }
}

#[cfg(feature = "crypto")]
#[derive(Default)]
pub struct MieruUdpFlowSessions {
    entries: MieruUdpFlowStore<crate::outbound::MieruUdpFlowConnection>,
}

#[cfg(feature = "crypto")]
impl MieruUdpFlowSessions {
    pub fn new() -> Self {
        Self {
            entries: MieruUdpFlowStore::new(),
        }
    }

    pub fn get(
        &self,
        resume: &MieruUdpFlowResume,
        server: &str,
        port: u16,
        session_id: u64,
    ) -> Option<&crate::outbound::MieruUdpFlowConnection> {
        self.entries.get(resume, server, port, session_id)
    }

    pub fn insert(
        &mut self,
        resume: &MieruUdpFlowResume,
        server: &str,
        port: u16,
        session_id: u64,
        connection: crate::outbound::MieruUdpFlowConnection,
    ) -> Option<crate::outbound::MieruUdpFlowConnection> {
        self.entries
            .insert(resume, server, port, session_id, connection)
    }
}

#[derive(Debug, Clone, Copy)]
struct MieruUdpPeerConfig<'a> {
    username: &'a str,
    password: &'a str,
}

impl<'a> MieruUdpPeerConfig<'a> {
    fn leaf_cache_key(&self, server: &str, port: u16) -> MieruUdpLeafKey {
        MieruUdpLeafKey {
            server: server.to_owned(),
            port,
            username: self.username.to_owned(),
            password: self.password.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MieruUdpLeafKey {
    server: String,
    port: u16,
    username: String,
    password: String,
}

impl DatagramCodec<Address> for MieruUdpFlowCodec {
    type Error = Error;

    fn encode(&self, target: &Address, port: u16, payload: &[u8]) -> Result<Vec<u8>, Self::Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    fn decode(&self, data: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        let decoded = decode_udp_flow_packet(data).ok()?;
        Some(decoded.into_parts())
    }
}
