//! Complete cold protocol profile; no socket, endpoint or private pool is built.
use super::*;
use zero_core::request::Destination;
use zero_platform_tokio::contract::network::UpstreamBinding;
#[derive(Clone)]
pub struct PoolProfile {
    pub(super) leaf: ProfileLeaf,
    pub(super) binding: UpstreamBinding,
    pub(super) endpoint: Destination,
    pub(super) identity: Arc<[u8]>,
}
#[derive(Clone)]
pub(super) struct ProfileLeaf {
    pub(super) tag: String,
    pub(super) server: String,
    pub(super) port: u16,
    pub(super) password: String,
    pub(super) insecure: bool,
    pub(super) client_fingerprint: Option<String>,
    pub(super) server_name: Option<String>,
    pub(super) settings: crate::settings::Settings,
    pub(super) node: super::super::model::Hysteria2NodeOptions,
}
impl PoolProfile {
    pub fn new(
        leaf: super::super::Hysteria2TransportLeaf,
        binding: UpstreamBinding,
        identity: Arc<[u8]>,
    ) -> Self {
        let endpoint = Destination::new(
            match zero_core::address::parse_ip_literal(&leaf.server) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(leaf.server.clone()),
            },
            leaf.port,
        );
        let leaf = ProfileLeaf {
            tag: leaf.tag,
            server: leaf.server,
            port: leaf.port,
            password: leaf.password,
            insecure: leaf.insecure,
            client_fingerprint: leaf.client_fingerprint,
            server_name: leaf.server_name,
            settings: leaf.settings,
            node: leaf.node,
        };
        Self {
            leaf,
            binding,
            endpoint,
            identity,
        }
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub fn wire_endpoint(&self) -> Destination {
        Destination::new(
            zero_core::Address::Domain(self.leaf.server.clone()),
            self.leaf.port,
        )
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub fn tag(&self) -> &str {
        &self.leaf.tag
    }
    pub(super) fn client_config(&self) -> Result<quinn::ClientConfig, ChannelError> {
        let leaf = &self.leaf;
        let mut config = zero_transport::quic::client_config_with_options(
            leaf.insecure,
            leaf.client_fingerprint.as_deref(),
            &super::super::outbound_quic_alpn_protocols(),
            Some(65536),
            leaf.node.ca_cert_path().as_deref(),
            &leaf.node.tls_options,
        )
        .map_err(|source| error(Stage::Prepare, Reason::InvalidConfig).with_source(source))?;
        config.transport_config(Arc::new(
            super::super::congestion::transport(leaf.settings).map_err(|source| {
                error(Stage::Prepare, Reason::InvalidConfig).with_source(source)
            })?,
        ));
        Ok(config)
    }
    pub(super) fn masks(&self) -> Vec<zero_transport::finalmask::udp::Mask> {
        self.leaf
            .node
            .salamander_password
            .as_ref()
            .map(
                |password| zero_transport::finalmask::udp::Mask::Salamander {
                    password: password.clone(),
                },
            )
            .into_iter()
            .collect()
    }
    pub(super) fn ports(&self) -> Vec<u16> {
        self.leaf
            .node
            .udp_hop
            .as_ref()
            .map(|hop| hop.ports().to_vec())
            .unwrap_or_default()
    }
}
