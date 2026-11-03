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

    pub(crate) fn masks(&self) -> Vec<zero_transport::finalmask::udp::Mask> {
        self.final_mask.udp().to_vec()
    }
    pub(crate) fn server_config(&self) -> Result<quic::QuicServerConfig, RuntimeError> {
        let profile = zero_transport::profile::OwnedServerTlsProfile {
            options: self.quic_tls_options.clone(),
            cert_path: self
                .quic_cert_path
                .clone()
                .ok_or_else(|| io::Error::other("QUIC certificate missing"))?,
            key_path: self
                .quic_key_path
                .clone()
                .ok_or_else(|| io::Error::other("QUIC private key missing"))?,
            alpn: Vec::new(),
            server_fingerprint: None,
        };
        let transport = if let Some(profile) = &self.hysteria {
            profile.transport(true)?
        } else {
            let mut t = quic::QuicTransportConfig::default();
            t.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
            t.datagram_receive_buffer_size(Some(65536));
            t
        };
        quic::server_config(
            &profile,
            self.source_dir.as_deref(),
            &self.quic_alpn_protocols,
            transport,
        )
    }
}
