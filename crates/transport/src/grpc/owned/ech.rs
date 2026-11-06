//! Dynamic TLS material consumes the same Node query owner, not a new DNS pool.
use super::*;
use crate::profile::OwnedClientTlsProfile;
use std::io;
fn dynamic_profile(tls: &OwnedClientTlsProfile) -> io::Result<bool> {
    Ok(matches!(
        tls.options.ech.source().map_err(io::Error::other)?,
        Some(zero_traits::EchConfigSource::Dns { .. })
    ))
}
pub(super) fn dynamic(options: &NativePoolOptions) -> io::Result<bool> {
    options
        .settings
        .tls
        .as_ref()
        .map(dynamic_profile)
        .transpose()
        .map(|value| value.unwrap_or(false))
}
/// Only for cold CA parsing. No connection can consume this temporary profile.
pub(super) fn validation_profile(tls: &OwnedClientTlsProfile) -> io::Result<OwnedClientTlsProfile> {
    crate::tls::OwnedClientRecipe::validation_profile(tls)
}
impl Managed {
    pub(super) async fn prepare_tls(
        &self,
        tls: &OwnedClientTlsProfile,
    ) -> io::Result<crate::tls::OwnedPreparedClient> {
        if !dynamic_profile(tls)? {
            return self
                .prepared_tls
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| io::Error::other("actual gRPC TLS owner has no prepared material"));
        }
        let resolver = self.ech_resolver.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "dynamic ECH has no actual Node query owner",
            )
        })?;
        let mut profile = tls.clone();
        crate::tls::ech::prepare_options(
            &mut profile.options,
            profile.server_name.as_deref(),
            &self.options.settings.server,
            resolver.as_ref(),
        )
        .await?;
        match &self.options.ca {
            Some(file) => file.prepare_material(&profile, &self.options.settings.server),
            None => crate::tls::OwnedPreparedClient::from_material(
                &profile,
                None,
                &self.options.settings.server,
                None,
            ),
        }
    }
}
