use alloc::format;
use alloc::string::String;

use base64::Engine;

pub const DEFAULT_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 32;
pub const DEFAULT_MUX_RESPONSE_BACKLOG_BYTES: u64 = 1024 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_FRAMES: u32 = 4096;
pub const MIN_MUX_RESPONSE_BACKLOG_BYTES: u64 = 16 * 1024;
pub const MAX_MUX_RESPONSE_BACKLOG_BYTES: u64 = 64 * 1024 * 1024;
pub use crate::flow_name::{
    FLOW_XTLS_RPRX_VISION, FLOW_XTLS_RPRX_VISION_UDP_LEGACY, FLOW_ZERO_AEAD_V1,
};

pub fn validate_flow(flow: &str) -> Result<(), &'static str> {
    match flow {
        FLOW_XTLS_RPRX_VISION | FLOW_ZERO_AEAD_V1 => Ok(()),
        FLOW_XTLS_RPRX_VISION_UDP_LEGACY => {
            Err("flow `xtls-rprx-vision-udp443` is obsolete; use `xtls-rprx-vision`")
        }
        _ => Err("flow must be `xtls-rprx-vision` or `zero-aead-v1`"),
    }
}

pub fn validate_mux_response_backlog(
    frames: Option<u32>,
    bytes: Option<u64>,
) -> Result<(), &'static str> {
    if frames.is_some_and(|value| value == 0 || value > MAX_MUX_RESPONSE_BACKLOG_FRAMES) {
        return Err("VLESS MUX response backlog frames must be within 1..=4096");
    }
    if bytes.is_some_and(|value| {
        !(MIN_MUX_RESPONSE_BACKLOG_BYTES..=MAX_MUX_RESPONSE_BACKLOG_BYTES).contains(&value)
    }) {
        return Err("VLESS MUX response backlog bytes must be within 16384..=67108864");
    }
    Ok(())
}

pub fn validate_reality_key(value: &str) -> Result<(), &'static str> {
    if value.contains('=') {
        return Err("must be base64url without padding");
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| "must be valid base64url without padding")?;
    if decoded.len() != 32 {
        return Err("must decode to exactly 32 bytes");
    }
    Ok(())
}

pub fn validate_reality_short_id(short_id: &str) -> Result<(), &'static str> {
    if short_id.len() > 16 {
        return Err("must be at most 16 hex characters");
    }
    if !short_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("must contain only hex digits");
    }
    Ok(())
}

pub fn validate_reality_cipher_suites(cipher_suites: &[String]) -> Result<(), String> {
    for cipher_suite in cipher_suites {
        match cipher_suite.as_str() {
            "TLS_AES_128_GCM_SHA256"
            | "TLS_AES_256_GCM_SHA384"
            | "TLS_CHACHA20_POLY1305_SHA256" => {}
            _ => return Err(format!("unsupported cipher suite `{cipher_suite}`")),
        }
    }
    Ok(())
}

pub fn validate_reality_client_fingerprint(value: &str) -> Result<(), String> {
    value
        .parse::<ztls::fingerprint::ClientHelloProfile>()
        .map(|_| ())
}

pub fn validate_xhttp_mode(mode: &str) -> Result<(), String> {
    match mode {
        "" | "auto" | "packet-up" | "stream-up" | "stream-one" => Ok(()),
        other => Err(format!(
            "mode `{other}` is not one of: auto, packet-up, stream-up, stream-one"
        )),
    }
}
