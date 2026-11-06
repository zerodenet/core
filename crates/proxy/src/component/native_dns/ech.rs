//! Cold typed ECH sources from the existing legal outbound TLS profiles.
use super::*;
use zero_config::{ClientTlsOptionsConfig, OutboundProtocolConfig};

pub(super) fn endpoints(
    candidate: &zero_config::RuntimeConfig,
) -> Result<Vec<zero_dns::EchDnsEndpoint>, RegistrationError> {
    let mut endpoints = Vec::new();
    for outbound in &candidate.outbounds {
        match &outbound.protocol {
            OutboundProtocolConfig::Vless {
                tls,
                quic,
                split_http,
                ..
            } => {
                if let Some(tls) = tls {
                    add(&tls.options, &mut endpoints)?;
                }
                if let Some(quic) = quic {
                    add(&quic.client_options, &mut endpoints)?;
                }
                if let Some(split) = split_http {
                    download(split, &mut endpoints)?;
                }
            }
            OutboundProtocolConfig::Vmess { tls: Some(tls), .. } => {
                add(&tls.options, &mut endpoints)?
            }
            OutboundProtocolConfig::Hysteria2 { tls_options, .. } => {
                add(tls_options, &mut endpoints)?
            }
            _ => {}
        }
    }
    Ok(endpoints)
}
fn download(
    split: &zero_config::SplitHttpConfig,
    endpoints: &mut Vec<zero_dns::EchDnsEndpoint>,
) -> Result<(), RegistrationError> {
    if let Some(download) = &split.download_settings {
        if let Some(tls) = &download.tls {
            add(&tls.options, endpoints)?;
        }
        if let Some(quic) = &download.quic {
            add(&quic.client_options, endpoints)?;
        }
        self::download(&download.split_http, endpoints)?;
    }
    Ok(())
}
fn add(
    options: &ClientTlsOptionsConfig,
    endpoints: &mut Vec<zero_dns::EchDnsEndpoint>,
) -> Result<(), RegistrationError> {
    let tls = options.to_options();
    if let Some(zero_traits::EchConfigSource::Dns { server, .. }) =
        tls.ech.source().map_err(invalid)?
    {
        let endpoint = zero_dns::EchDnsEndpoint::parse(server).map_err(invalid)?;
        if !endpoints.contains(&endpoint) {
            endpoints.push(endpoint);
        }
    }
    Ok(())
}
fn invalid(source: impl ToString) -> RegistrationError {
    RegistrationError::Options(zero_config::ConfigError::InvalidOutbound(
        source.to_string(),
    ))
}

pub(super) fn destination(endpoint: &zero_dns::EchDnsEndpoint) -> zero_core::request::Destination {
    endpoint
        .literal_peer()
        .map(provider::destination)
        .unwrap_or_else(|| {
            zero_core::request::Destination::new(
                zero_core::Address::Domain(endpoint.host().to_owned()),
                endpoint.port(),
            )
        })
}
