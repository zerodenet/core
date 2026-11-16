//! mKCP uses the same actual packet owner; it has no invented QUIC profile.
use super::*;
impl Options {
    pub(super) fn from_mkcp_parts(
        parts: crate::transport::OwnedMkcpParts,
        runtime: &VlessTransportRuntime,
        dial: DialPolicy,
        origins: NonZeroUsize,
        users: NonZeroUsize,
        limits: MetadataLimits,
        source_dir: Option<&std::path::Path>,
        file_bytes: NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        dial.validate().map_err(std::io::Error::other)?;
        parts.settings.validate()?;
        let (uuid, flow, testseed) = parts.protocol;
        let mut identity = Vec::from(parts.identity);
        identity.extend_from_slice(b"actual.vless.mkcp.original.v1");
        use std::hash::{BuildHasher, Hash, Hasher};
        static KEYS: std::sync::OnceLock<std::collections::hash_map::RandomState> =
            std::sync::OnceLock::new();
        let mut hash = KEYS.get_or_init(Default::default).build_hasher();
        (&dial, uuid, flow, testseed).hash(&mut hash);
        identity.extend_from_slice(&hash.finish().to_be_bytes());
        let protocol = Protocol {
            material: None,
            masks: None,
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
            // The existing packet plan applies Encryption exactly once after
            // the actual reliable stream's optional TLS handshake.
            encryption: None,
            tls: parts.tls,
            ws: None,
            grpc: None,
            h2: None,
            http_upgrade: None,
            dial,
            identity: identity.into(),
        }
        .with_owned_material(runtime, source_dir, file_bytes)?;
        let mut suffix = Vec::new();
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
        let mut result = Self {
            protocol,
            plan: parts.plan,
            profile: None,
            mkcp: Some(parts.settings),
            hysteria: None,
            h3: None,
            carrier: None,
            origins,
            users,
            limits,
            identity: Arc::from([]),
            suffix: suffix.into(),
            raw_masks: None,
            buffers: Default::default(),
            dependencies: vec![],
        };
        result.refresh_material_identity();
        Ok(result)
    }
}
