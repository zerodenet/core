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
        tag: &str,
        server: &str,
        port: u16,
        options: super::super::Hysteria2OutboundOptionsRef<'_>,
        binding: UpstreamBinding,
        identity: Arc<[u8]>,
    ) -> Self {
        let endpoint = Destination::new(
            match zero_core::address::parse_ip_literal(server) {
                Some(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
                Some(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
                None => zero_core::Address::Domain(server.to_owned()),
            },
            port,
        );
        let leaf = ProfileLeaf {
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            password: options.password.to_owned(),
            insecure: options.insecure,
            client_fingerprint: options.client_fingerprint.map(ToOwned::to_owned),
            server_name: options.server_name.map(ToOwned::to_owned),
            settings: options.settings,
            node: Default::default(),
        };
        Self {
            leaf,
            binding,
            endpoint,
            identity,
        }
    }
    pub fn with_ca_cert_path(mut self, path: Option<&str>) -> Self {
        self.leaf.node.ca_cert_path = path.map(ToOwned::to_owned);
        self
    }
    pub fn ca_cert_path(&self) -> Option<std::path::PathBuf> {
        self.leaf.node.ca_cert_path()
    }
    pub fn with_tls_options(mut self, options: zero_traits::ClientTlsOptions) -> Self {
        self.leaf.node.tls_options = options;
        self
    }
    pub fn with_salamander_password(mut self, password: Option<&str>) -> Self {
        self.leaf.node.salamander_password = password.map(ToOwned::to_owned);
        self
    }
    pub fn with_udp_hop(mut self, hop: Option<zero_transport::datagram_hop::Profile>) -> Self {
        self.leaf.node.udp_hop = hop;
        self
    }
    pub fn with_source_dir(mut self, directory: Option<&std::path::Path>) -> Self {
        self.leaf.node.source_dir = directory.map(ToOwned::to_owned);
        self
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
    pub(super) fn client_config(
        &self,
        ca_pem: Option<&[u8]>,
    ) -> Result<quinn::ClientConfig, ChannelError> {
        let leaf = &self.leaf;
        let mut config = zero_transport::quic::client_config_with_ca_material(
            leaf.insecure,
            leaf.client_fingerprint.as_deref(),
            &super::super::outbound_quic_alpn_protocols(),
            Some(65536),
            leaf.node.ca_cert_path().as_deref(),
            &leaf.node.tls_options,
            ca_pem,
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
