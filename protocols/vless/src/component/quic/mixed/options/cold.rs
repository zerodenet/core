//! Same accepted pair's actual material receipts; identity is never a grant.
use super::*;
impl Options {
    pub fn endpoint(&self) -> &zero_core::request::Destination {
        &self.upload.protocol().endpoint
    }
    pub fn dial(&self) -> &zero_traits::DialPolicy {
        &self.upload.protocol().dial
    }
    pub fn download_endpoint(&self) -> Option<&zero_core::request::Destination> {
        self.download.as_ref().map(|half| &half.protocol().endpoint)
    }
    pub fn download_dial(&self) -> Option<&zero_traits::DialPolicy> {
        self.download.as_ref().map(|half| &half.protocol().dial)
    }
    pub fn with_node_dns(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.dependencies.push(dependency);
        self
    }
    pub fn packet_path_metadata(&self) -> (zero_core::request::Destination, bool) {
        (self.endpoint().clone(), true)
    }
    pub fn needs_node_dns(&self) -> bool {
        std::iter::once(&self.upload)
            .chain(self.download.iter())
            .any(|half| {
                let options = half.protocol();
                zero_core::address::parse_ip_literal(&options.server).is_none()
                    || options.tls.as_ref().is_some_and(|tls| {
                        matches!(
                            tls.options.ech.source(),
                            Ok(Some(zero_traits::EchConfigSource::Dns { .. }))
                        )
                    })
            })
    }
    pub fn remember_cold_file_material(&self) -> Result<(), zero_transport::RuntimeError> {
        for half in std::iter::once(&self.upload).chain(self.download.iter()) {
            half.protocol()
                .remember_cold_material_identity(&self.identity)?;
        }
        Ok(())
    }
    pub fn reuse_file_material_from(
        &mut self,
        previous: zero_core::component::ComponentInstanceId,
    ) -> Result<bool, zero_transport::RuntimeError> {
        let mut all = true;
        for half in std::iter::once(&mut self.upload).chain(self.download.iter_mut()) {
            all &= half.protocol_mut().reuse_file_material_from(previous)?;
        }
        self.refresh_identity();
        Ok(all)
    }
    pub fn reuse_cold_file_material_from(
        &mut self,
        identity: &[u8],
    ) -> Result<bool, zero_transport::RuntimeError> {
        let mut all = true;
        for half in std::iter::once(&mut self.upload).chain(self.download.iter_mut()) {
            all &= half
                .protocol_mut()
                .reuse_cold_file_material_from(identity)?;
        }
        self.refresh_identity();
        Ok(all)
    }
    pub(super) fn refresh_identity(&mut self) {
        let mut identity = self.recipe.to_vec();
        identity.extend_from_slice(b"vless.actual-owned-xhttp-pair.v1");
        for half in std::iter::once(&self.upload).chain(self.download.iter()) {
            identity.extend_from_slice(half.protocol().identity.as_ref());
        }
        for dimension in self.dimensions {
            identity.extend_from_slice(&dimension.to_le_bytes());
        }
        self.identity = identity.into();
    }
}
impl Half {
    pub(in crate::component::quic::mixed) fn protocol(&self) -> &crate::component::Options {
        match self {
            Self::Quic(options) => &options.protocol,
            Self::Tcp(options) => &options.protocol,
        }
    }
    fn protocol_mut(&mut self) -> &mut crate::component::Options {
        match self {
            Self::Quic(options) => {
                &mut Arc::get_mut(options)
                    .expect("cold unshared half options")
                    .protocol
            }
            Self::Tcp(options) => {
                &mut Arc::get_mut(options)
                    .expect("cold unshared half options")
                    .protocol
            }
        }
    }
    pub(in crate::component::quic::mixed) fn pool(
        &self,
    ) -> (&XhttpClientPool, &OwnedSplitHttpProfile) {
        match self {
            Self::Quic(options) => {
                let (pool, profile) = options.h3.as_ref().expect("actual H3 half");
                (pool, profile)
            }
            Self::Tcp(options) => (&options.pool, &options.http),
        }
    }
    pub(in crate::component::quic::mixed) fn kind(
        &self,
    ) -> zero_transport::split_http::OwnedConnectionKind {
        match self {
            Self::Tcp(options)
                if matches!(
                    options.version,
                    zero_transport::split_http::TcpVersion::Http1
                ) =>
            {
                zero_transport::split_http::OwnedConnectionKind::Http1
            }
            _ => zero_transport::split_http::OwnedConnectionKind::Multiplexed,
        }
    }
}
