//! TLS parameter validation without compiling the TLS data plane.
use base64::Engine;
use zero_traits::{ClientTlsOptions, ServerTlsOptions, TlsParameters};

pub fn version(value: &str, default: u16) -> Result<u16, String> {
    match value {
        "" => Ok(default),
        "1.0" => Ok(0x301),
        "1.1" => Ok(0x302),
        "1.2" => Ok(0x303),
        "1.3" => Ok(0x304),
        _ => Err(format!(
            "unsupported TLS version {value:?}; supported versions are 1.0, 1.1, 1.2 and 1.3"
        )),
    }
}
pub fn cipher_suite(value: &str) -> Result<u16, String> {
    Ok(match value {
        "TLS_AES_128_GCM_SHA256" => 0x1301,
        "TLS_AES_256_GCM_SHA384" => 0x1302,
        "TLS_CHACHA20_POLY1305_SHA256" => 0x1303,
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => 0xc02b,
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => 0xc02c,
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => 0xc02f,
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => 0xc030,
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA" => 0xc009,
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA" => 0xc00a,
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" => 0xc013,
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA" => 0xc014,
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"
        | "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305" => 0xcca9,
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" | "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305" => {
            0xcca8
        }
        _ => return Err(format!("unsupported TLS cipher suite {value:?}")),
    })
}
pub fn curve(value: &str) -> Result<u16, String> {
    Ok(match value {
        "X25519" | "x25519" => 29,
        "P256" | "p256" | "secp256r1" | "CurveP256" | "curvep256" => 23,
        "P384" | "p384" | "secp384r1" | "CurveP384" | "curvep384" => 24,
        "P521" | "p521" | "secp521r1" | "CurveP521" | "curvep521" => 25,
        "X25519MLKEM768" | "x25519mlkem768" => 4588,
        "SecP256r1MLKEM768" | "secp256r1mlkem768" => 4587,
        "SecP384r1MLKEM1024" | "secp384r1mlkem1024" => 4589,
        _ => return Err(format!("unsupported TLS key exchange group {value:?}")),
    })
}
pub fn certificate_pin(value: &str) -> Result<[u8; 32], String> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(value))
        .map_err(|_| "TLS certificate pin must be base64 SHA-256".to_string())?
        .try_into()
        .map_err(|_| "TLS certificate pin must contain 32 bytes".to_string())
}
pub fn uses_legacy_versions(options: &TlsParameters) -> Result<bool, String> {
    let min = version(&options.min_version, 0x303)?;
    let max = version(&options.max_version, 0x304)?;
    Ok(min < 0x303 || max < 0x303)
}

pub fn needs_openssl_parameters(options: &TlsParameters) -> Result<bool, String> {
    if uses_legacy_versions(options)? {
        return Ok(true);
    }
    options
        .cipher_suites
        .iter()
        .try_fold(false, |needed, name| {
            let suite = cipher_suite(name)?;
            Ok(needed || matches!(suite, 0xc009 | 0xc00a | 0xc013 | 0xc014))
        })
}

pub fn validate_parameters(options: &TlsParameters, quic: bool) -> Result<(), String> {
    let min = version(&options.min_version, if quic { 0x304 } else { 0x303 })?;
    let max = version(&options.max_version, 0x304)?;
    if min > max || (quic && (min != 0x304 || max != 0x304)) {
        return Err("TLS version range excludes this carrier".into());
    }
    for name in &options.cipher_suites {
        cipher_suite(name)?;
    }
    for name in &options.curve_preferences {
        curve(name)?;
    }
    Ok(())
}
pub fn validate_client(options: &ClientTlsOptions, quic: bool) -> Result<(), String> {
    validate_parameters(&options.parameters, quic)?;
    let source = options.ech.source()?;
    if source.is_none()
        && options.backend == zero_traits::TlsBackend::Rustls
        && needs_openssl_parameters(&options.parameters)?
    {
        return Err("rustls does not support the configured legacy TLS parameters".into());
    }
    if let Some(source) = source {
        if options.backend == zero_traits::TlsBackend::OpenSsl {
            return Err("ECH client requires the rustls TLS backend".into());
        }
        let max = &options.parameters.max_version;
        if !max.is_empty() && max != "1.3" {
            return Err("ECH requires TLS 1.3".into());
        }
        match source {
            zero_traits::EchConfigSource::Static(value) => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(value)
                    .map_err(|_| "ECH config list must be base64 or a DNS source".to_owned())?;
                if decoded.is_empty() {
                    return Err("ECH config list must not be empty".into());
                }
                validate_ech_config_list(&decoded)?;
            }
            zero_traits::EchConfigSource::Dns { query_name, server } => {
                if let Some(query_name) = query_name {
                    ech_query_name(query_name)?;
                }
                validate_ech_dns_server(server)?;
            }
        }
    }
    for pin in &options.pinned_peer_cert_sha256 {
        certificate_pin(pin)?;
    }
    for name in &options.verify_peer_names {
        if name.is_empty()
            || name
                .bytes()
                .any(|b| b <= 32 || b >= 127 || b"/*?#@".contains(&b))
        {
            return Err("TLS verification name must be a DNS name or IP address".into());
        }
    }
    Ok(())
}

pub fn ech_query_name(value: &str) -> Result<(), String> {
    let value = value.strip_suffix('.').unwrap_or(value);
    if value.is_empty()
        || value.len() > 253
        || value.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        return Err("ECH query name must be a DNS name".into());
    }
    Ok(())
}

fn validate_ech_dns_server(value: &str) -> Result<(), String> {
    let endpoint = url::Url::parse(value).map_err(|_| "ECH DNS endpoint URL is invalid")?;
    if endpoint.host_str().is_none()
        || endpoint.port() == Some(0)
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err("ECH DNS endpoint URL is invalid".into());
    }
    if endpoint.scheme() == "udp"
        && ((!endpoint.path().is_empty() && endpoint.path() != "/") || endpoint.query().is_some())
    {
        return Err("UDP ECH DNS endpoint cannot have a path or query".into());
    }
    Ok(())
}

fn validate_ech_config_list(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() < 6 {
        return Err("ECH config list is truncated".into());
    }
    let list_len = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    if list_len != bytes.len() - 2 {
        return Err("ECH config list length is invalid".into());
    }
    let mut offset = 2;
    while offset < bytes.len() {
        if offset + 4 > bytes.len() {
            return Err("ECH config entry is truncated".into());
        }
        let config_len = usize::from(u16::from_be_bytes([bytes[offset + 2], bytes[offset + 3]]));
        offset = offset
            .checked_add(4 + config_len)
            .ok_or_else(|| "ECH config entry length overflow".to_owned())?;
        if config_len == 0 || offset > bytes.len() {
            return Err("ECH config entry length is invalid".into());
        }
    }
    Ok(())
}

pub fn validate_server(options: &ServerTlsOptions, quic: bool) -> Result<(), String> {
    validate_parameters(&options.parameters, quic)?;
    if options.backend == zero_traits::TlsBackend::Rustls
        && (needs_openssl_parameters(&options.parameters)? || !options.ech_server_keys.is_empty())
    {
        return Err("rustls does not support TLS 1.0, TLS 1.1 or ECH server keys".into());
    }
    if !options.ech_server_keys.is_empty() {
        if version(&options.parameters.max_version, 0x304)? < 0x304 {
            return Err("ECH server keys require TLS 1.3 in the configured version range".into());
        }
        validate_ech_server_keys(&options.ech_server_keys)?;
    }
    if options.reload_interval_secs > i64::MAX as u64 / 1_000_000_000
        || options.ocsp_stapling_secs > i64::MAX as u64 / 1_000_000_000
        || options
            .certificates
            .iter()
            .any(|cert| cert.ocsp_stapling_secs > i64::MAX as u64 / 1_000_000_000)
    {
        return Err("TLS certificate reload interval exceeds duration limit".into());
    }
    for cert in &options.certificates {
        if cert.cert_path.is_empty()
            || cert.key_path.is_empty()
            || cert.ocsp_path.as_ref().is_some_and(String::is_empty)
        {
            return Err("TLS certificate paths must not be empty".into());
        }
    }
    Ok(())
}

fn validate_ech_server_keys(encoded: &str) -> Result<(), String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| "ECH server keys must be standard base64".to_string())?;
    if bytes.is_empty() {
        return Err("ECH server keys must contain at least one key set".into());
    }
    let mut position = 0usize;
    while position < bytes.len() {
        let key_length = read_u16(&bytes, position)?;
        position += 2;
        let key_end = position
            .checked_add(key_length)
            .ok_or_else(|| "ECH server keys length overflow".to_string())?;
        if key_length == 0 || key_end + 2 > bytes.len() {
            return Err("ECH server keys contain a truncated private key".into());
        }
        position = key_end;
        let config_length = read_u16(&bytes, position)?;
        position += 2;
        let config_end = position
            .checked_add(config_length)
            .ok_or_else(|| "ECH server keys length overflow".to_string())?;
        if config_length < 7 || config_end > bytes.len() {
            return Err("ECH server keys contain a truncated ECHConfig".into());
        }
        let config = &bytes[position..config_end];
        if config[0..2] != [0xfe, 0x0d] {
            return Err("ECH server keys require RFC 9849 ECHConfig version 0xfe0d".into());
        }
        let contents_length = usize::from(u16::from_be_bytes([config[2], config[3]]));
        if contents_length + 4 != config.len() {
            return Err("ECH server keys contain an invalid ECHConfig length".into());
        }
        if key_length != 32 || config[5..7] != [0x00, 0x20] {
            return Err("ECH server keys must contain X25519 key sets".into());
        }
        position = config_end;
    }
    Ok(())
}

fn read_u16(bytes: &[u8], position: usize) -> Result<usize, String> {
    let value = bytes
        .get(position..position + 2)
        .ok_or_else(|| "ECH server keys are truncated".to_string())?;
    Ok(usize::from(u16::from_be_bytes([value[0], value[1]])))
}
