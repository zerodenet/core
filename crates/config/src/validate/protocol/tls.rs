use crate::{
    ConfigError, InboundProtocolConfig, OutboundProtocolConfig, ServerTlsOptionsConfig,
    TlsBackendConfig,
};

fn server_material(
    scope: &str,
    cert_path: &str,
    key_path: &str,
    options: &ServerTlsOptionsConfig,
) -> Result<(), ConfigError> {
    let has_cert = !cert_path.trim().is_empty();
    let has_key = !key_path.trim().is_empty();
    if has_cert != has_key {
        return Err(ConfigError::InvalidInbound(format!(
            "`{scope}` certificate and key paths must be configured together"
        )));
    }
    if !has_cert && options.certificates.is_empty() {
        return Err(ConfigError::InvalidInbound(format!(
            "`{scope}` requires a certificate or authority-issue entry"
        )));
    }
    Ok(())
}

pub(super) fn inbound(protocol: &InboundProtocolConfig) -> Result<(), ConfigError> {
    let tls = match protocol {
        InboundProtocolConfig::Vless { tls, .. } | InboundProtocolConfig::Vmess { tls, .. } => {
            tls.as_deref()
        }
        InboundProtocolConfig::Trojan { tls, .. } => tls.as_ref(),
        _ => None,
    };
    if let Some(tls) = tls {
        server_material("tls", &tls.cert_path, &tls.key_path, &tls.options)?;
        ztls::settings::validate_server(&tls.options.to_options(), false)
            .map_err(ConfigError::InvalidInbound)?;
        let openssl = tls.options.backend == TlsBackendConfig::OpenSsl
            || (tls.options.backend == TlsBackendConfig::Auto
                && (ztls::settings::needs_openssl_parameters(
                    &tls.options.parameters.to_options(),
                )
                .map_err(ConfigError::InvalidInbound)?
                    || !tls.options.ech_server_keys.is_empty()));
        if openssl && tls.server_fingerprint.is_some() {
            return Err(ConfigError::InvalidInbound(
                "OpenSSL TLS cannot apply a rustls server fingerprint preset".into(),
            ));
        }
    }
    if let InboundProtocolConfig::Vless {
        quic: Some(quic), ..
    } = protocol
    {
        server_material(
            "quic",
            quic.cert_path.as_deref().unwrap_or(""),
            quic.key_path.as_deref().unwrap_or(""),
            &quic.server_options,
        )?;
        ztls::settings::validate_server(&quic.server_options.to_options(), true)
            .map_err(ConfigError::InvalidInbound)?;
    }
    Ok(())
}
pub(super) fn outbound(protocol: &OutboundProtocolConfig) -> Result<(), ConfigError> {
    let (tls, default_server) = match protocol {
        OutboundProtocolConfig::Vless { tls, server, .. }
        | OutboundProtocolConfig::Vmess { tls, server, .. } => {
            (tls.as_deref(), Some(server.as_str()))
        }
        _ => (None, None),
    };
    if let Some(tls) = tls {
        let options = tls.options.to_options();
        ztls::settings::validate_client(&options, false).map_err(ConfigError::InvalidOutbound)?;
        validate_ech_name(
            &options,
            tls.server_name.as_deref().or(default_server),
            tls.disable_sni,
        )?;
        let openssl = tls.options.backend == TlsBackendConfig::OpenSsl
            || (tls.options.backend == TlsBackendConfig::Auto
                && ztls::settings::needs_openssl_parameters(&tls.options.parameters.to_options())
                    .map_err(ConfigError::InvalidOutbound)?);
        if openssl && tls.client_fingerprint.is_some() {
            return Err(ConfigError::InvalidOutbound(
                "OpenSSL TLS cannot apply a rustls ClientHello fingerprint preset".into(),
            ));
        }
    }
    if let OutboundProtocolConfig::Vless {
        quic: Some(quic), ..
    } = protocol
    {
        let options = quic.client_options.to_options();
        ztls::settings::validate_client(&options, true).map_err(ConfigError::InvalidOutbound)?;
        validate_ech_name(
            &options,
            quic.server_name.as_deref().or(default_server),
            false,
        )?;
    }
    Ok(())
}

pub(in crate::validate) fn validate_ech_name(
    options: &zero_traits::ClientTlsOptions,
    server_name: Option<&str>,
    disable_sni: bool,
) -> Result<(), ConfigError> {
    let source = options
        .ech
        .source()
        .map_err(|error| ConfigError::InvalidOutbound(error.into()))?;
    if source.is_none() {
        return Ok(());
    }
    if disable_sni {
        return Err(ConfigError::InvalidOutbound(
            "ECH cannot be combined with disable_sni".into(),
        ));
    }
    if let Some(zero_traits::EchConfigSource::Dns {
        query_name: None, ..
    }) = source
    {
        let name = server_name
            .filter(|name| name.parse::<std::net::IpAddr>().is_err())
            .ok_or_else(|| {
                ConfigError::InvalidOutbound(
                    "DNS-backed ECH requires a DNS server_name or explicit query-name prefix"
                        .into(),
                )
            })?;
        ztls::settings::ech_query_name(name).map_err(ConfigError::InvalidOutbound)?;
    }
    Ok(())
}
