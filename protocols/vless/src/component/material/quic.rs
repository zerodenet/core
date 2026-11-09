use super::*;
impl Material {
    pub(in crate::component) async fn prepare_quic(
        &self,
        resolver: Option<&dyn zero_transport::tls::ech::EchConfigResolver>,
        alpn: &[Vec<u8>],
    ) -> std::io::Result<zero_transport::quic::QuicClientConfig> {
        if self.prepared.lock().unwrap().is_none() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "actual VLESS material owner not started",
            ));
        }
        let mut profile = self.profile.clone();
        if matches!(
            profile
                .options
                .ech
                .source()
                .map_err(std::io::Error::other)?,
            Some(zero_traits::EchConfigSource::Dns { .. })
        ) {
            let resolver = resolver.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "actual Node ECH resolver absent",
                )
            })?;
            zero_transport::tls::ech::prepare_options(
                &mut profile.options,
                profile.server_name.as_deref(),
                &self.server,
                resolver,
            )
            .await?;
        }
        match &self.file {
            Some(file) => file.prepare_quic_material(&profile, alpn),
            None => zero_transport::quic::client_config_with_ca_material(
                profile.insecure,
                None,
                alpn,
                None,
                None,
                &profile.options,
                None,
            )
            .map_err(std::io::Error::other),
        }
    }
}
