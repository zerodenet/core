use crate::{ConfigError, SplitHttpConfig};
use zero_traits::SplitHttpTransportProfile;

pub(super) fn validate(config: &SplitHttpConfig, inbound: bool) -> Result<(), ConfigError> {
    let fail = |message: &str| {
        let message = format!("vless split_http.{message}");
        if inbound {
            ConfigError::InvalidInbound(message)
        } else {
            ConfigError::InvalidOutbound(message)
        }
    };
    if let Some(browser) = &config.browser_dialer {
        browser.validate().map_err(&fail)?;
        if inbound {
            return Err(fail("browser_dialer is outbound-only"));
        }
        if !matches!(config.mode.as_str(), "" | "auto" | "packet-up") {
            return Err(fail("browser_dialer supports packet-up mode only"));
        }
        if config.download_settings.is_some() {
            return Err(fail("browser_dialer does not support download_settings"));
        }
    }
    if let Some(download) = &config.download_settings {
        if inbound || config.mode == "stream-one" {
            return Err(fail(
                "download_settings requires an outbound with packet-up, stream-up or auto mode",
            ));
        }
        if download.server.trim().is_empty() || download.port == 0 {
            return Err(fail("download_settings requires a server and nonzero port"));
        }
        if download.split_http.download_settings.is_some() {
            return Err(fail(
                "download_settings cannot contain another download_settings",
            ));
        }
        if [
            download.tls.is_some(),
            download.reality.is_some(),
            download.quic.is_some(),
        ]
        .into_iter()
        .filter(|v| *v)
        .count()
            > 1
        {
            return Err(fail("download_settings accepts only one security carrier"));
        }
        if let Some(reality) = &download.reality {
            super::protocol::validate_vless_reality(reality)?;
        }
        if let Some(tls) = &download.tls {
            let options = tls.options.to_options();
            ztls::settings::validate_client(&options, false)
                .map_err(ConfigError::InvalidOutbound)?;
            super::protocol::validate_ech_name(
                &options,
                tls.server_name.as_deref().or(Some(&download.server)),
                tls.disable_sni,
            )?;
        }
        if let Some(quic) = &download.quic {
            let options = quic.client_options.to_options();
            ztls::settings::validate_client(&options, true)
                .map_err(ConfigError::InvalidOutbound)?;
            super::protocol::validate_ech_name(
                &options,
                quic.server_name.as_deref().or(Some(&download.server)),
                false,
            )?;
        }
        let tls_fields = download
            .tls
            .as_ref()
            .map(|tls| (&tls.server_name, &tls.ca_cert_path))
            .or_else(|| {
                download
                    .quic
                    .as_ref()
                    .map(|quic| (&quic.server_name, &quic.ca_cert_path))
            });
        if let Some((name, ca)) = tls_fields {
            if name.as_ref().is_some_and(|s| s.trim().is_empty())
                || ca.as_ref().is_some_and(|s| s.trim().is_empty())
            {
                return Err(fail(
                    "download_settings TLS server name and CA path cannot be empty",
                ));
            }
        }
        validate(&download.split_http, false)?;
    }
    let o = config.options();
    for range in [
        o.xmux.max_concurrency,
        o.xmux.max_connections,
        o.xmux.c_max_reuse_times,
        o.xmux.h_max_request_times,
        o.xmux.h_max_reusable_secs,
    ] {
        if range.from > range.to || range.to > i32::MAX as u32 {
            return Err(fail("xmux ranges require 0 <= from <= to <= 2147483647"));
        }
    }
    if o.xmux.max_concurrency.to > 0 && o.xmux.max_connections.to > 0 {
        return Err(fail(
            "xmux max_concurrency and max_connections are mutually exclusive",
        ));
    }
    if o.xmux.h_keep_alive_period > i32::MAX as i64 {
        return Err(fail("xmux h_keep_alive_period exceeds timer capacity"));
    }
    if o.sc_max_each_post_bytes.from == 0 && o.sc_max_each_post_bytes.to > 0 {
        return Err(fail("sc_max_each_post_bytes must be positive"));
    }
    for (name, r, cap) in [
        ("x_padding_bytes", o.x_padding_bytes, 1024 * 1024),
        ("uplink_chunk_size", o.uplink_chunk_size, 16 * 1024 * 1024),
        (
            "sc_max_each_post_bytes",
            o.sc_max_each_post_bytes,
            16 * 1024 * 1024,
        ),
        (
            "sc_min_posts_interval_ms",
            o.sc_min_posts_interval_ms,
            86_400_000,
        ),
        (
            "sc_stream_up_server_secs",
            o.sc_stream_up_server_secs,
            86_400,
        ),
    ] {
        if r.from > r.to || r.to > cap {
            return Err(fail(&format!("{name} requires 0 <= from <= to <= {cap}")));
        }
    }
    for (name, value, allowed) in [
        (
            "session_placement",
            o.session_placement.as_str(),
            &["", "path", "query", "header", "cookie"][..],
        ),
        (
            "seq_placement",
            o.seq_placement.as_str(),
            &["", "path", "query", "header", "cookie"][..],
        ),
        (
            "uplink_data_placement",
            o.uplink_data_placement.as_str(),
            &["", "body", "auto", "header", "cookie"][..],
        ),
        (
            "x_padding_placement",
            o.x_padding_placement.as_str(),
            &["queryInHeader", "query", "header", "cookie"][..],
        ),
        (
            "x_padding_method",
            o.x_padding_method.as_str(),
            &["", "repeat-x", "tokenish"][..],
        ),
    ] {
        if !allowed.contains(&value) {
            return Err(fail(&format!("{name} is unsupported")));
        }
    }
    if o.sc_max_buffered_posts > 65536 || o.server_max_header_bytes > 16 * 1024 * 1024 {
        return Err(fail("buffer/header limits exceed transport capacity"));
    }
    fn token(s: &str) -> bool {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    }
    if !o.uplink_http_method.is_empty()
        && (!token(&o.uplink_http_method) || o.uplink_http_method == "OPTIONS")
    {
        return Err(fail(
            "uplink_http_method must be an HTTP method other than OPTIONS",
        ));
    }
    if (o.uplink_http_method.eq_ignore_ascii_case("GET")
        || ["header", "cookie"].contains(&o.uplink_data_placement.as_str()))
        && config.mode != "packet-up"
    {
        return Err(fail("GET or header/cookie uploads require packet-up mode"));
    }
    if o.x_padding_bytes.from == 0 && o.x_padding_bytes.to != 0 {
        return Err(fail("x_padding_bytes cannot include zero"));
    }
    for (name, key, placement) in [
        ("session_key", &o.session_key, &o.session_placement),
        ("seq_key", &o.seq_key, &o.seq_placement),
        (
            "uplink_data_key",
            &o.uplink_data_key,
            &o.uplink_data_placement,
        ),
    ] {
        if !key.is_empty() && ["header", "cookie"].contains(&placement.as_str()) && !token(key) {
            return Err(fail(&format!("{name} must be an HTTP token")));
        }
    }
    if o.x_padding_obfs_mode
        && (!token(&o.x_padding_header)
            || o.x_padding_key.is_empty()
            || (o.x_padding_placement == "cookie" && !token(&o.x_padding_key)))
    {
        return Err(fail("padding header/key is invalid"));
    }
    for (key, value) in &o.headers {
        if !token(key) || value.bytes().any(|c| (c < 32 && c != b'\t') || c == 127) {
            return Err(fail("headers contain an invalid HTTP name/value"));
        }
        if [
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "upgrade",
        ]
        .iter()
        .any(|s| key.eq_ignore_ascii_case(s))
        {
            return Err(fail("headers cannot override HTTP framing"));
        }
    }
    Ok(())
}
