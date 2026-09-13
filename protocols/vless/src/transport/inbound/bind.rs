use std::io;
use std::path::{Path, PathBuf};

use super::super::options::VlessQuicBindOptionsRef;
use super::super::profile::VlessQuicBindProfile;
use zero_transport::RuntimeError;

use zero_transport::quic;

#[derive(Debug, Clone, Default)]
pub struct VlessInboundBindPlan {
    quic_tls_options: zero_traits::ServerTlsOptions,
    quic_cert_path: Option<String>,
    quic_key_path: Option<String>,
    quic_alpn_protocols: Vec<Vec<u8>>,
    source_dir: Option<PathBuf>,
    hysteria: Option<zero_transport::hysteria::Profile>,
    final_mask: zero_transport::finalmask::Profile,
}

impl VlessInboundBindPlan {
    pub fn with_final_mask(
        mut self,
        masks: Vec<zero_transport::finalmask::udp::Mask>,
    ) -> Result<Self, RuntimeError> {
        self.final_mask = zero_transport::finalmask::Profile::from_udp(masks)?;
        Ok(self)
    }

    pub fn with_hysteria(
        mut self,
        options: Option<zero_transport::hysteria::OptionsRef<'_>>,
    ) -> Result<Self, RuntimeError> {
        self.hysteria = options
            .map(zero_transport::hysteria::Profile::from_options)
            .transpose()?;
        Ok(self)
    }

    /// A split-HTTP profile over a QUIC listener negotiates the HTTP/3 ALPN.
    pub fn with_http3(mut self, enabled: bool) -> Self {
        if enabled {
            self.quic_alpn_protocols = vec![b"h3".to_vec()];
        }
        self
    }
    pub fn from_options_refs(
        source_dir: Option<&Path>,
        quic: Option<VlessQuicBindOptionsRef<'_>>,
    ) -> Self {
        let quic = quic.map(VlessQuicBindProfile::from);
        Self::from_quic_profile(source_dir, quic.as_ref())
    }

    fn from_quic_profile(source_dir: Option<&Path>, quic: Option<&VlessQuicBindProfile>) -> Self {
        Self {
            quic_tls_options: quic.map(|q| q.tls_options.clone()).unwrap_or_default(),
            quic_cert_path: quic.and_then(|config| config.cert_path.clone()),
            quic_key_path: quic.and_then(|config| config.key_path.clone()),
            quic_alpn_protocols: quic
                .map(VlessQuicBindProfile::alpn_protocols)
                .unwrap_or_default(),
            source_dir: source_dir.map(PathBuf::from),
            hysteria: None,
            final_mask: Default::default(),
        }
    }

    pub async fn bind(&self, listen_addr: &str) -> Result<Option<quic::QuicInbound>, RuntimeError> {
        match (
            self.quic_cert_path.as_deref(),
            self.quic_key_path.as_deref(),
        ) {
            (Some(cert_path), Some(key_path)) => {
                let profile = zero_transport::profile::OwnedServerTlsProfile {
                    options: self.quic_tls_options.clone(),
                    cert_path: cert_path.into(),
                    key_path: key_path.into(),
                    alpn: Vec::new(),
                    server_fingerprint: None,
                };
                let transport = if let Some(profile) = &self.hysteria {
                    profile.transport(true)?
                } else {
                    let mut transport = quic::QuicTransportConfig::default();
                    transport.max_idle_timeout(Some(
                        std::time::Duration::from_secs(30).try_into().unwrap(),
                    ));
                    transport.datagram_receive_buffer_size(Some(65536));
                    transport
                };
                let endpoint = quic::QuicInbound::bind_with_tls_profile(
                    listen_addr,
                    &profile,
                    self.source_dir.as_deref(),
                    &self.quic_alpn_protocols,
                    transport,
                    self.final_mask.udp(),
                )
                .await?;
                Ok(Some(endpoint))
            }
            (None, None) => Ok(None),
            _ => Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "vless quic inbound bind requires both cert_path and key_path",
            ))),
        }
    }
}
