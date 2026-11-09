use super::*;
use crate::component::Options as Protocol;
use crate::transport::{VlessOutboundLeaf, VlessTransportRuntime};
use zero_platform_tokio::controlled_datagram::metadata::MetadataLimits;
use zero_traits::DialPolicy;
use zero_transport::profile::OwnedClientTlsProfile;
pub struct Options {
    pub(super) protocol: Protocol,
    pub(super) plan: crate::transport::OwnedVlessOutboundTransportPlan,
    pub(super) profile: crate::transport::VlessQuicClientProfile,
    pub(super) origins: NonZeroUsize,
    pub(super) users: NonZeroUsize,
    pub(super) limits: MetadataLimits,
    pub(super) identity: Arc<[u8]>,
    suffix: Arc<[u8]>,
    pub(super) buffers: zero_platform_tokio::controlled_carrier::CarrierBufferLimits,
    pub(super) dependencies: Vec<zero_core::component::ComponentId>,
}
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessOwnedQuic")
            .field("endpoint", &self.protocol.endpoint)
            .finish_non_exhaustive()
    }
}
impl Options {
    /// Consume the real validated leaf data, never claim/decode the config a
    /// second time. None leaves the other existing transport selection intact.
    pub fn from_accepted_leaf(
        leaf: &VlessOutboundLeaf,
        runtime: &VlessTransportRuntime,
        dial: DialPolicy,
        origins: NonZeroUsize,
        users: NonZeroUsize,
        limits: MetadataLimits,
        source_dir: Option<&std::path::Path>,
        file_bytes: NonZeroUsize,
    ) -> Result<Option<Self>, zero_transport::RuntimeError> {
        let Some(parts) = leaf.plain_owned_quic_parts() else {
            return Ok(None);
        };
        dial.validate().map_err(std::io::Error::other)?;
        let profile = parts.profile;
        let tls = OwnedClientTlsProfile {
            server_name: profile.server_name.clone(),
            insecure: profile.insecure,
            ca_cert_path: profile.ca_cert_path.clone(),
            options: profile.tls_options.clone(),
            disable_sni: false,
            alpn: profile
                .alpn_protocols()
                .iter()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect(),
            client_fingerprint: None,
        };
        let (uuid, flow, testseed) = parts.protocol;
        let mut identity = Vec::from(parts.identity);
        identity.extend_from_slice(b"actual.vless.quic.original.v1");
        // Same actual protocol values, independent from transport pool security
        // keys. Static identity is opaque process-local metadata, never authority.
        use std::hash::{BuildHasher, Hash, Hasher};
        static KEYS: std::sync::OnceLock<std::collections::hash_map::RandomState> =
            std::sync::OnceLock::new();
        let mut hash = KEYS.get_or_init(Default::default).build_hasher();
        (uuid, flow, testseed, &dial).hash(&mut hash);
        identity.extend_from_slice(&hash.finish().to_be_bytes());
        let protocol = Protocol {
            material: None,
            server: match &parts.endpoint.address {
                zero_core::Address::Domain(name) => name.clone(),
                zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(*ip).to_string(),
                zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(*ip).to_string(),
            },
            endpoint: parts.endpoint,
            uuid,
            flow,
            testseed,
            reality: None,
            tls: Some(tls),
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            dial,
            identity: identity.into(),
        }
        .with_owned_material(runtime, source_dir, file_bytes)?;
        let mut suffix = Vec::new();
        let mut identity = protocol.identity.to_vec();
        for dimension in [
            origins.get(),
            users.get(),
            limits.main_sockets.get(),
            limits.probe_sockets.get(),
            limits.probe_attempts_per_initialization.get(),
            limits.concurrent_send_calls.get(),
            limits.write_pollers.get(),
        ] {
            suffix.extend_from_slice(&(dimension as u64).to_be_bytes());
        }
        identity.extend_from_slice(&suffix);
        let mut prepared = Self {
            suffix: suffix.into(),
            buffers: Default::default(),
            dependencies: vec![],
            protocol,
            plan: parts.plan,
            profile,
            origins,
            users,
            limits,
            identity: identity.into(),
        };
        prepared.refresh_material_identity();
        Ok(Some(prepared))
    }
    pub fn packet_path_metadata(&self) -> (zero_core::request::Destination, bool) {
        (self.protocol.endpoint.clone(), true)
    }
    pub fn requires_icmp(&self) -> bool {
        self.plan
            .owned_quic_masks()
            .iter()
            .any(|mask| matches!(mask, zero_transport::finalmask::udp::Mask::Xicmp { .. }))
    }
    pub fn reuse_file_material_from(
        &mut self,
        previous: zero_core::component::ComponentInstanceId,
    ) -> Result<bool, zero_transport::RuntimeError> {
        let reused = self.protocol.reuse_file_material_from(previous)?;
        if reused {
            self.refresh_material_identity();
        }
        Ok(reused)
    }
    pub fn endpoint(&self) -> &zero_core::request::Destination {
        &self.protocol.endpoint
    }
    pub fn dial(&self) -> &DialPolicy {
        &self.protocol.dial
    }
    pub fn with_node_dns(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.dependencies.push(dependency);
        self
    }
    pub fn with_buffer_policy(
        mut self,
        buffers: zero_platform_tokio::controlled_carrier::CarrierBufferLimits,
    ) -> Result<Self, zero_transport::RuntimeError> {
        buffers.validate()?;
        self.buffers = buffers;
        self.refresh_material_identity();
        Ok(self)
    }
    pub fn remember_cold_file_material(&self) -> Result<(), zero_transport::RuntimeError> {
        Ok(self
            .protocol
            .remember_cold_material_identity(&self.identity)?)
    }
    pub fn reuse_cold_file_material_from(
        &mut self,
        identity: &[u8],
    ) -> Result<bool, zero_transport::RuntimeError> {
        let reused = self.protocol.reuse_cold_file_material_from(identity)?;
        if reused {
            self.refresh_material_identity();
        }
        Ok(reused)
    }
    fn refresh_material_identity(&mut self) {
        let mut identity = self.protocol.identity.to_vec();
        identity.extend_from_slice(&self.suffix);
        identity.extend_from_slice(&(self.buffers.max_queue_packets as u64).to_be_bytes());
        identity.extend_from_slice(&(self.buffers.total_max_bytes as u64).to_be_bytes());
        self.identity = identity.into();
    }
}
