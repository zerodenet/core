use std::collections::HashSet;

use crate::{
    ConfigError, Hysteria2UserConfig, InboundProtocolConfig, InboundRealityConfig, MieruUserConfig,
    OutboundProtocolConfig, RealityConfig, ShadowsocksUserConfig, Socks5UserConfig,
    TrojanUserConfig, VlessUserConfig, VmessUserConfig,
};

pub(super) fn validate_inbound_protocol(
    protocol: &InboundProtocolConfig,
) -> Result<(), ConfigError> {
    match protocol {
        InboundProtocolConfig::Socks5 { users } => validate_socks5_users("socks5 inbound", users),
        InboundProtocolConfig::Mixed { socks5_users } => {
            validate_socks5_users("mixed inbound socks5", socks5_users)
        }
        InboundProtocolConfig::HttpConnect => Ok(()),
        InboundProtocolConfig::Vless {
            users,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            tls,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            fallback: _,
            quic,
            split_http,
        } => {
            validate_vless_users(users)?;
            if users
                .iter()
                .any(|user| user.flow.as_deref() == Some(vless::validation::FLOW_XTLS_RPRX_VISION))
                && reality.is_none()
            {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound flow `xtls-rprx-vision` requires `reality`".to_owned(),
                ));
            }
            if let Some(tls) = tls {
                validate_inbound_optional_non_empty("vless tls.cert_path", &tls.cert_path)?;
                validate_inbound_optional_non_empty("vless tls.key_path", &tls.key_path)?;
            }
            if let Some(reality) = reality {
                validate_vless_inbound_reality(reality)?;
            }
            if tls.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound cannot set both `tls` and `reality`".to_owned(),
                ));
            }
            if ws.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound `reality` supports raw TCP only, not `ws`".to_owned(),
                ));
            }
            if grpc.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound `reality` supports raw TCP only, not `grpc`".to_owned(),
                ));
            }
            if h2.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound `reality` supports raw TCP only, not `h2`".to_owned(),
                ));
            }
            if http_upgrade.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound `reality` supports raw TCP only, not `http_upgrade`"
                        .to_owned(),
                ));
            }
            if quic.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vless` inbound `reality` supports raw TCP only, not `quic`".to_owned(),
                ));
            }
            if let Some(ws) = ws {
                validate_inbound_optional_non_empty("vless ws.path", &ws.path)?;
                validate_inbound_ws_headers("vless ws.headers", &ws.headers)?;
            }
            if let Some(grpc) = grpc {
                for name in &grpc.service_names {
                    validate_inbound_optional_non_empty("vless grpc.service_names", name)?;
                }
            }
            if let Some(h2) = h2 {
                validate_inbound_optional_non_empty("vless h2.path", &h2.path)?;
            }
            if let Some(http_upgrade) = http_upgrade {
                validate_inbound_optional_non_empty("vless http_upgrade.path", &http_upgrade.path)?;
            }
            if let Some(quic) = quic {
                if let Some(cert_path) = &quic.cert_path {
                    validate_inbound_optional_non_empty("vless quic.cert_path", cert_path)?;
                }
                if let Some(key_path) = &quic.key_path {
                    validate_inbound_optional_non_empty("vless quic.key_path", key_path)?;
                }
            }
            if let Some(split_http) = split_http {
                validate_xhttp_mode("inbound", &split_http.mode)?;
            }
            validate_mux_response_backlog(
                "vless inbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                vless::validation::validate_mux_response_backlog,
            )?;
            Ok(())
        }
        InboundProtocolConfig::Hysteria2 {
            password,
            users,
            cert_path,
            key_path,
            ..
        } => {
            validate_hysteria2_users(password, users)?;
            if cert_path.is_some() != key_path.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "hysteria2 tls requires both cert_path and key_path, or neither".to_owned(),
                ));
            }
            if let Some(cert_path) = cert_path {
                validate_inbound_optional_non_empty("hysteria2 cert_path", cert_path)?;
            }
            if let Some(key_path) = key_path {
                validate_inbound_optional_non_empty("hysteria2 key_path", key_path)?;
            }
            Ok(())
        }
        InboundProtocolConfig::Shadowsocks {
            password,
            identity_password,
            users,
            cipher,
            ..
        } => {
            validate_shadowsocks_cipher("inbound", cipher)?;
            validate_shadowsocks_users(password, identity_password.as_deref(), users, cipher)
        }
        InboundProtocolConfig::Trojan {
            password,
            users,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            validate_trojan_users(password, users)?;
            validate_mux_response_backlog(
                "trojan inbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                trojan::validation::validate_mux_response_backlog,
            )
        }
        InboundProtocolConfig::Vmess {
            users,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            tls,
            ws,
            grpc,
        } => {
            validate_vmess_users(users)?;
            let tls = tls.as_ref().ok_or_else(|| {
                ConfigError::InvalidInbound("`vmess` inbound requires `tls`".to_owned())
            })?;
            validate_inbound_optional_non_empty("vmess tls.cert_path", &tls.cert_path)?;
            validate_inbound_optional_non_empty("vmess tls.key_path", &tls.key_path)?;
            if ws.is_some() && grpc.is_some() {
                return Err(ConfigError::InvalidInbound(
                    "`vmess` inbound cannot set both `ws` and `grpc`".to_owned(),
                ));
            }
            if let Some(ws) = ws {
                validate_inbound_optional_non_empty("vmess ws.path", &ws.path)?;
                validate_inbound_ws_headers("vmess ws.headers", &ws.headers)?;
            }
            if let Some(grpc) = grpc {
                for name in &grpc.service_names {
                    validate_inbound_optional_non_empty("vmess grpc.service_names", name)?;
                }
            }
            validate_mux_response_backlog(
                "vmess inbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                vmess::validation::validate_mux_response_backlog,
            )?;
            Ok(())
        }
        InboundProtocolConfig::Direct { .. } => Ok(()),
        InboundProtocolConfig::Mieru { users } => validate_mieru_users(users),
    }
}

pub(super) fn validate_outbound_protocol(
    protocol: &OutboundProtocolConfig,
) -> Result<(), ConfigError> {
    match protocol {
        OutboundProtocolConfig::Socks5 {
            username, password, ..
        } => validate_socks5_outbound_auth(username.as_deref(), password.as_deref()),
        OutboundProtocolConfig::Vless {
            server,
            port,
            id,
            flow,
            mux_concurrency,
            xudp_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            tls,
            reality,
            ws,
            grpc,
            h2,
            http_upgrade,
            quic,
            split_http,
        } => {
            validate_outbound_endpoint("vless", server, *port)?;
            vless::parse_uuid(id).map_err(|error| {
                let message = error.to_string();
                ConfigError::InvalidOutbound(format!("`vless` outbound `id` {message}"))
            })?;
            if let Some(flow) = flow {
                vless::validation::validate_flow(flow).map_err(|message| {
                    ConfigError::InvalidOutbound(format!("`vless` outbound {message}"))
                })?;
                if flow == vless::validation::FLOW_XTLS_RPRX_VISION && reality.is_none() {
                    return Err(ConfigError::InvalidOutbound(
                        "`vless` outbound flow `xtls-rprx-vision` requires `reality`".to_owned(),
                    ));
                }
                if flow == vless::validation::FLOW_XTLS_RPRX_VISION && mux_concurrency.is_some() {
                    return Err(ConfigError::InvalidOutbound(
                        "`vless` outbound flow `xtls-rprx-vision` cannot be combined with `mux_concurrency`"
                            .to_owned(),
                    ));
                }
            }
            if let Some(tls) = tls {
                if let Some(server_name) = &tls.server_name {
                    validate_outbound_optional_non_empty("vless tls.server_name", server_name)?;
                }
                if let Some(ca_cert_path) = &tls.ca_cert_path {
                    validate_outbound_optional_non_empty("vless tls.ca_cert_path", ca_cert_path)?;
                }
            }
            if let Some(reality) = reality {
                validate_vless_reality(reality)?;
            }
            if tls.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound cannot set both `tls` and `reality`".to_owned(),
                ));
            }
            if ws.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound `reality` supports raw TCP only, not `ws`".to_owned(),
                ));
            }
            if grpc.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound `reality` supports raw TCP only, not `grpc`".to_owned(),
                ));
            }
            if h2.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound `reality` supports raw TCP only, not `h2`".to_owned(),
                ));
            }
            if http_upgrade.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound `reality` supports raw TCP only, not `http_upgrade`"
                        .to_owned(),
                ));
            }
            if quic.is_some() && reality.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vless` outbound `reality` supports raw TCP only, not `quic`".to_owned(),
                ));
            }
            if let Some(ws) = ws {
                validate_outbound_optional_non_empty("vless ws.path", &ws.path)?;
                validate_outbound_ws_headers("vless ws.headers", &ws.headers)?;
            }
            if let Some(grpc) = grpc {
                for name in &grpc.service_names {
                    validate_outbound_optional_non_empty("vless grpc.service_names", name)?;
                }
            }
            if let Some(h2) = h2 {
                validate_outbound_optional_non_empty("vless h2.path", &h2.path)?;
            }
            if let Some(http_upgrade) = http_upgrade {
                validate_outbound_optional_non_empty(
                    "vless http_upgrade.path",
                    &http_upgrade.path,
                )?;
            }
            if let Some(quic) = quic {
                if let Some(server_name) = &quic.server_name {
                    validate_outbound_optional_non_empty("vless quic.server_name", server_name)?;
                }
            }
            if let Some(split_http) = split_http {
                validate_xhttp_mode("outbound", &split_http.mode)?;
            }
            validate_optional_mux_concurrency("vless mux_concurrency", *mux_concurrency)?;
            validate_optional_mux_concurrency("vless xudp_concurrency", *xudp_concurrency)?;
            validate_optional_positive("vless mux_idle_timeout_secs", *mux_idle_timeout_secs)?;
            validate_mux_response_backlog(
                "vless outbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                vless::validation::validate_mux_response_backlog,
            )?;
            Ok(())
        }
        OutboundProtocolConfig::Hysteria2 { server, port, .. } => {
            validate_outbound_endpoint("hysteria2", server, *port)?;
            Ok(())
        }
        OutboundProtocolConfig::Shadowsocks {
            server,
            port,
            password,
            cipher,
        } => {
            validate_outbound_endpoint("shadowsocks", server, *port)?;
            validate_outbound_optional_non_empty("shadowsocks password", password)?;
            validate_shadowsocks_cipher("outbound", cipher)?;
            validate_shadowsocks_password("outbound", cipher, password)?;
            Ok(())
        }
        OutboundProtocolConfig::Trojan {
            server,
            port,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            ..
        } => {
            validate_outbound_endpoint("trojan", server, *port)?;
            validate_optional_positive("trojan mux_concurrency", mux_concurrency.map(u64::from))?;
            validate_optional_positive("trojan mux_idle_timeout_secs", *mux_idle_timeout_secs)?;
            validate_mux_response_backlog(
                "trojan outbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                trojan::validation::validate_mux_response_backlog,
            )?;
            Ok(())
        }
        OutboundProtocolConfig::Vmess {
            server,
            port,
            id,
            cipher,
            mux_concurrency,
            mux_idle_timeout_secs,
            mux_response_backlog_frames,
            mux_response_backlog_bytes,
            tls,
            ws,
            grpc,
        } => {
            validate_outbound_endpoint("vmess", server, *port)?;
            vmess::parse_uuid(id).map_err(|error| {
                ConfigError::InvalidOutbound(format!("`vmess` outbound `id` {error}"))
            })?;
            validate_vmess_cipher("outbound", cipher)?;
            if let Some(tls) = tls {
                if let Some(server_name) = &tls.server_name {
                    validate_outbound_optional_non_empty("vmess tls.server_name", server_name)?;
                }
                if let Some(ca_cert_path) = &tls.ca_cert_path {
                    validate_outbound_optional_non_empty("vmess tls.ca_cert_path", ca_cert_path)?;
                }
            }
            if ws.is_some() && grpc.is_some() {
                return Err(ConfigError::InvalidOutbound(
                    "`vmess` outbound cannot set both `ws` and `grpc`".to_owned(),
                ));
            }
            validate_optional_positive("vmess mux_concurrency", mux_concurrency.map(u64::from))?;
            validate_optional_positive("vmess mux_idle_timeout_secs", *mux_idle_timeout_secs)?;
            validate_mux_response_backlog(
                "vmess outbound",
                *mux_response_backlog_frames,
                *mux_response_backlog_bytes,
                vmess::validation::validate_mux_response_backlog,
            )?;
            if let Some(ws) = ws {
                validate_outbound_optional_non_empty("vmess ws.path", &ws.path)?;
                validate_outbound_ws_headers("vmess ws.headers", &ws.headers)?;
            }
            if let Some(grpc) = grpc {
                for name in &grpc.service_names {
                    validate_outbound_optional_non_empty("vmess grpc.service_names", name)?;
                }
            }
            Ok(())
        }
        OutboundProtocolConfig::Direct | OutboundProtocolConfig::Block => Ok(()),
        OutboundProtocolConfig::Mieru {
            server,
            port,
            username,
            password,
        } => {
            validate_outbound_endpoint("mieru", server, *port)?;
            validate_outbound_optional_non_empty("mieru password", password)?;
            if let Some(username) = username {
                validate_outbound_optional_non_empty("mieru username", username)?;
            }
            Ok(())
        }
    }
}

fn validate_mieru_users(users: &[MieruUserConfig]) -> Result<(), ConfigError> {
    if users.is_empty() {
        return Err(ConfigError::InvalidInbound(
            "`mieru` inbound requires at least one user".to_owned(),
        ));
    }

    let mut usernames = HashSet::new();
    let mut principals = HashSet::new();
    for user in users {
        validate_inbound_optional_non_empty("mieru username", &user.username)?;
        validate_inbound_optional_non_empty("mieru password", &user.password)?;
        if !usernames.insert(user.username.as_str()) {
            return Err(ConfigError::InvalidInbound(format!(
                "`mieru` inbound contains duplicate username `{}`",
                user.username
            )));
        }
        let principal_key = user.principal_key.as_deref().unwrap_or(&user.username);
        validate_inbound_optional_non_empty("mieru principal_key", principal_key)?;
        if !principals.insert(principal_key) {
            return Err(ConfigError::InvalidInbound(format!(
                "`mieru` inbound contains duplicate effective principal_key `{principal_key}`"
            )));
        }
    }

    Ok(())
}

fn validate_vless_users(users: &[VlessUserConfig]) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();
    for user in users {
        vless::parse_uuid(&user.id).map_err(|error| {
            let message = error.to_string();
            ConfigError::InvalidInbound(format!("`vless` inbound user `id` {message}"))
        })?;

        if !seen.insert(normalize_uuid_key(&user.id)) {
            return Err(ConfigError::InvalidInbound(
                "`vless` inbound contains duplicate user id".to_owned(),
            ));
        }

        if let Some(principal_key) = &user.principal_key {
            validate_inbound_optional_non_empty("vless principal_key", principal_key)?;
        }
        if let Some(flow) = &user.flow {
            vless::validation::validate_flow(flow).map_err(|message| {
                ConfigError::InvalidInbound(format!("`vless` inbound user {message}"))
            })?;
        }
    }

    Ok(())
}

fn validate_vmess_users(users: &[VmessUserConfig]) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();
    for user in users {
        vmess::parse_uuid(&user.id).map_err(|error| {
            let message = error.to_string();
            ConfigError::InvalidInbound(format!("`vmess` inbound user `id` {message}"))
        })?;

        if !seen.insert(normalize_uuid_key(&user.id)) {
            return Err(ConfigError::InvalidInbound(
                "`vmess` inbound contains duplicate user id".to_owned(),
            ));
        }

        validate_vmess_cipher("inbound", &user.cipher)?;
        if let Some(principal_key) = &user.principal_key {
            validate_inbound_optional_non_empty("vmess principal_key", principal_key)?;
        }
    }

    Ok(())
}

fn validate_trojan_users(
    legacy_password: &str,
    users: &[TrojanUserConfig],
) -> Result<(), ConfigError> {
    if users.is_empty() {
        return if legacy_password.is_empty() {
            Ok(())
        } else {
            validate_inbound_optional_non_empty("trojan password", legacy_password)
        };
    }
    if !legacy_password.is_empty() {
        return Err(ConfigError::InvalidInbound(
            "`trojan` inbound cannot configure both legacy `password` and `users`".to_owned(),
        ));
    }

    let mut passwords = HashSet::new();
    let mut principals = HashSet::new();
    for user in users {
        validate_inbound_optional_non_empty("trojan user password", &user.password)?;
        if !passwords.insert(user.password.as_str()) {
            return Err(ConfigError::InvalidInbound(
                "`trojan` inbound contains duplicate user password".to_owned(),
            ));
        }
        if let Some(principal_key) = user.principal_key.as_deref() {
            validate_inbound_optional_non_empty("trojan principal_key", principal_key)?;
            if !principals.insert(principal_key) {
                return Err(ConfigError::InvalidInbound(format!(
                    "`trojan` inbound contains duplicate principal_key `{principal_key}`"
                )));
            }
        }
    }
    Ok(())
}

fn validate_hysteria2_users(
    legacy_password: &str,
    users: &[Hysteria2UserConfig],
) -> Result<(), ConfigError> {
    if users.is_empty() {
        return if legacy_password.is_empty() {
            Ok(())
        } else {
            validate_inbound_optional_non_empty("hysteria2 password", legacy_password)
        };
    }
    if !legacy_password.is_empty() {
        return Err(ConfigError::InvalidInbound(
            "`hysteria2` inbound cannot configure both legacy `password` and `users`".to_owned(),
        ));
    }

    let mut passwords = HashSet::new();
    let mut principals = HashSet::new();
    for user in users {
        validate_inbound_optional_non_empty("hysteria2 user password", &user.password)?;
        if !passwords.insert(user.password.as_str()) {
            return Err(ConfigError::InvalidInbound(
                "`hysteria2` inbound contains duplicate user password".to_owned(),
            ));
        }
        if let Some(principal_key) = user.principal_key.as_deref() {
            validate_inbound_optional_non_empty("hysteria2 principal_key", principal_key)?;
            if !principals.insert(principal_key) {
                return Err(ConfigError::InvalidInbound(format!(
                    "`hysteria2` inbound contains duplicate principal_key `{principal_key}`"
                )));
            }
        }
    }
    Ok(())
}

fn validate_shadowsocks_users(
    legacy_password: &str,
    identity_password: Option<&str>,
    users: &[ShadowsocksUserConfig],
    cipher: &str,
) -> Result<(), ConfigError> {
    if users.is_empty() {
        if let Some(identity_password) = identity_password {
            if !legacy_password.is_empty() {
                return Err(ConfigError::InvalidInbound(
                    "`shadowsocks` inbound cannot configure both `password` and `identity_password`"
                        .to_owned(),
                ));
            }
            validate_inbound_optional_non_empty(
                "shadowsocks identity_password",
                identity_password,
            )?;
            if identity_password.contains(':') {
                return Err(ConfigError::InvalidInbound(
                    "`shadowsocks.identity_password` must contain exactly one 2022 PSK".to_owned(),
                ));
            }
            if !cipher.starts_with("2022-blake3-aes-") {
                return Err(ConfigError::InvalidInbound(
                    "`shadowsocks.identity_password` requires a 2022 AES cipher".to_owned(),
                ));
            }
            validate_shadowsocks_password("inbound identity", cipher, identity_password)?;
        }
        return if legacy_password.is_empty() || identity_password.is_some() {
            Ok(())
        } else {
            validate_inbound_optional_non_empty("shadowsocks password", legacy_password)?;
            if cipher.starts_with("2022-") && legacy_password.contains(':') {
                return Err(ConfigError::InvalidInbound(
                    "`shadowsocks` inbound single-user password must contain exactly one 2022 PSK"
                        .to_owned(),
                ));
            }
            validate_shadowsocks_password("inbound", cipher, legacy_password)
        };
    }
    if cipher.starts_with("2022-") {
        if cipher == "2022-blake3-chacha20-poly1305" {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` SIP023 EIH multi-user mode requires a 2022 AES cipher".to_owned(),
            ));
        }
        if !legacy_password.is_empty() {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` 2022 managed inbound uses `identity_password`; legacy `password` must be empty"
                    .to_owned(),
            ));
        }
        let Some(identity_password) = identity_password else {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` 2022 multi-user inbound requires `identity_password` as the SIP023 server identity PSK"
                    .to_owned(),
            ));
        };
        validate_inbound_optional_non_empty("shadowsocks identity_password", identity_password)?;
        if identity_password.contains(':') {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks.identity_password` must contain exactly one 2022 PSK".to_owned(),
            ));
        }
        validate_shadowsocks_password("inbound identity", cipher, identity_password)?;
    } else if !legacy_password.is_empty() {
        return Err(ConfigError::InvalidInbound(
            "`shadowsocks` legacy AEAD inbound cannot configure both `password` and `users`"
                .to_owned(),
        ));
    }

    let mut passwords = HashSet::new();
    let mut principals = HashSet::new();
    for user in users {
        validate_inbound_optional_non_empty("shadowsocks user password", &user.password)?;
        if cipher.starts_with("2022-") && user.password.contains(':') {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` managed user password must contain exactly one 2022 uPSK".to_owned(),
            ));
        }
        validate_shadowsocks_password("inbound", cipher, &user.password)?;
        if identity_password.is_some_and(|identity| identity == user.password) {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` SIP023 server identity PSK must differ from every user PSK"
                    .to_owned(),
            ));
        }
        if !passwords.insert(user.password.as_str()) {
            return Err(ConfigError::InvalidInbound(
                "`shadowsocks` inbound contains duplicate user password".to_owned(),
            ));
        }
        if let Some(principal_key) = user.principal_key.as_deref() {
            validate_inbound_optional_non_empty("shadowsocks principal_key", principal_key)?;
            if !principals.insert(principal_key) {
                return Err(ConfigError::InvalidInbound(format!(
                    "`shadowsocks` inbound contains duplicate principal_key `{principal_key}`"
                )));
            }
        }
    }
    Ok(())
}

fn validate_vmess_cipher(kind: &'static str, cipher: &str) -> Result<(), ConfigError> {
    let valid_ciphers = ["aes-128-gcm", "chacha20-poly1305", "none", "zero"];
    if cipher != "auto" && vmess::VmessCipher::from_name(cipher).is_some() {
        return Ok(());
    }

    let message = format!(
        "`vmess` {kind} cipher `{cipher}` is not valid; expected one of: {}",
        valid_ciphers.join(", ")
    );
    match kind {
        "inbound" => Err(ConfigError::InvalidInbound(message)),
        _ => Err(ConfigError::InvalidOutbound(message)),
    }
}

fn validate_vless_inbound_reality(reality: &InboundRealityConfig) -> Result<(), ConfigError> {
    validate_inbound_optional_non_empty("vless reality.private_key", &reality.private_key)?;
    if vless::validation::validate_reality_key(&reality.private_key).is_err() {
        return Err(ConfigError::InvalidInbound(
            "`vless` inbound `reality.private_key` must be a 32-byte base64url value without padding"
                .to_owned(),
        ));
    }

    for short_id in &reality.short_ids {
        vless::validation::validate_reality_short_id(short_id).map_err(|error| {
            ConfigError::InvalidInbound(format!("`vless` inbound `reality.short_id` {error}"))
        })?;
    }

    if let Some(server_name) = &reality.server_name {
        validate_inbound_optional_non_empty("vless reality.server_name", server_name)?;
    }

    vless::validation::validate_reality_cipher_suites(&reality.cipher_suites).map_err(|error| {
        ConfigError::InvalidInbound(format!(
            "`vless` inbound `reality.cipher_suites` contains {error}"
        ))
    })
}

fn validate_vless_reality(reality: &RealityConfig) -> Result<(), ConfigError> {
    validate_outbound_optional_non_empty("vless reality.public_key", &reality.public_key)?;
    if vless::validation::validate_reality_key(&reality.public_key).is_err() {
        return Err(ConfigError::InvalidOutbound(
            "`vless` outbound `reality.public_key` must be a 32-byte base64url value without padding"
                .to_owned(),
        ));
    }

    vless::validation::validate_reality_short_id(&reality.short_id).map_err(|error| {
        ConfigError::InvalidOutbound(format!("`vless` outbound `reality.short_id` {error}"))
    })?;

    if let Some(server_name) = &reality.server_name {
        validate_outbound_optional_non_empty("vless reality.server_name", server_name)?;
    }

    vless::validation::validate_reality_cipher_suites(&reality.cipher_suites).map_err(|error| {
        ConfigError::InvalidOutbound(format!(
            "`vless` outbound `reality.cipher_suites` contains {error}"
        ))
    })?;
    vless::validation::validate_reality_client_fingerprint(&reality.client_fingerprint).map_err(
        |error| {
            ConfigError::InvalidOutbound(format!(
                "`vless` outbound `reality.client_fingerprint` {error}"
            ))
        },
    )
}

/// Validate the XHTTP `mode` field on a `vless` `split_http` transport config.
///
/// `auto` / `stream-one` resolve to the single-connection path; `packet-up` /
/// `stream-up` select the legacy two-connection model. Any other value is
/// rejected. An empty string is treated as the default `auto`.
fn validate_xhttp_mode(kind: &str, mode: &str) -> Result<(), ConfigError> {
    let ctor = if kind == "inbound" {
        ConfigError::InvalidInbound
    } else {
        ConfigError::InvalidOutbound
    };
    vless::validation::validate_xhttp_mode(mode)
        .map_err(|error| ctor(format!("`vless` {kind} split_http.{error}")))
}

fn validate_outbound_endpoint(
    protocol: &'static str,
    server: &str,
    port: u16,
) -> Result<(), ConfigError> {
    if server.trim().is_empty() {
        return Err(ConfigError::InvalidOutbound(format!(
            "`{protocol}` outbound requires a non-empty `server`"
        )));
    }

    if port == 0 {
        return Err(ConfigError::InvalidOutbound(format!(
            "`{protocol}` outbound `port` must be greater than 0"
        )));
    }

    Ok(())
}

fn validate_socks5_users(
    scope: &'static str,
    users: &[Socks5UserConfig],
) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();

    for user in users {
        validate_socks5_credential_part(scope, "username", &user.username)?;
        validate_socks5_credential_part(scope, "password", &user.password)?;
        if !seen.insert(user.username.as_str()) {
            return Err(ConfigError::InvalidInbound(format!(
                "`{scope}` contains duplicate username `{}`",
                user.username
            )));
        }
    }

    Ok(())
}

fn validate_socks5_outbound_auth(
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), ConfigError> {
    match (username, password) {
        (None, None) => Ok(()),
        (Some(username), Some(password)) => {
            validate_socks5_outbound_credential_part("username", username)?;
            validate_socks5_outbound_credential_part("password", password)
        }
        _ => Err(ConfigError::InvalidOutbound(
            "`socks5` outbound requires both `username` and `password`, or neither".to_owned(),
        )),
    }
}

fn validate_socks5_outbound_credential_part(
    field: &'static str,
    value: &str,
) -> Result<(), ConfigError> {
    socks5::validate_credential_part(value, field).map_err(|error| {
        ConfigError::InvalidOutbound(format!("`socks5` outbound `{field}` is invalid: {error}"))
    })
}

fn validate_socks5_credential_part(
    scope: &'static str,
    field: &'static str,
    value: &str,
) -> Result<(), ConfigError> {
    socks5::validate_credential_part(value, field).map_err(|error| {
        ConfigError::InvalidInbound(format!("`{scope}` `{field}` is invalid: {error}"))
    })
}

fn validate_inbound_optional_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidInbound(format!(
            "`{field}` must not be empty"
        )));
    }

    Ok(())
}

fn validate_outbound_optional_non_empty(
    field: &'static str,
    value: &str,
) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidOutbound(format!(
            "`{field}` must not be empty"
        )));
    }

    Ok(())
}

fn validate_inbound_ws_headers(
    field: &'static str,
    headers: &std::collections::HashMap<String, String>,
) -> Result<(), ConfigError> {
    for key in headers.keys() {
        let key_lower = key.to_lowercase();
        if is_reserved_ws_header(&key_lower) {
            return Err(ConfigError::InvalidInbound(format!(
                "`{field}` contains reserved header `{key}` which is managed by WebSocket handshake",
            )));
        }
    }

    Ok(())
}

fn validate_outbound_ws_headers(
    field: &'static str,
    headers: &std::collections::HashMap<String, String>,
) -> Result<(), ConfigError> {
    for key in headers.keys() {
        let key_lower = key.to_lowercase();
        if is_reserved_ws_header(&key_lower) {
            return Err(ConfigError::InvalidOutbound(format!(
                "`{field}` contains reserved header `{key}` which is managed by WebSocket handshake",
            )));
        }
    }

    Ok(())
}

fn is_reserved_ws_header(header: &str) -> bool {
    const RESERVED_HEADERS: &[&str] = &[
        "host",
        "connection",
        "upgrade",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-protocol",
        "sec-websocket-extensions",
        "sec-websocket-accept",
    ];
    RESERVED_HEADERS.contains(&header)
}

fn normalize_uuid_key(value: &str) -> String {
    value
        .bytes()
        .filter(|byte| *byte != b'-')
        .map(|byte| char::from(byte.to_ascii_lowercase()))
        .collect()
}

fn validate_shadowsocks_cipher(kind: &'static str, cipher: &str) -> Result<(), ConfigError> {
    const VALID_CIPHERS: &[&str] = &[
        "aes-128-gcm",
        "aes-256-gcm",
        "chacha20-ietf-poly1305",
        "2022-blake3-aes-128-gcm",
        "2022-blake3-aes-256-gcm",
        "2022-blake3-chacha20-poly1305",
    ];
    if shadowsocks::validation::validate_cipher(cipher).is_err() {
        return Err(match kind {
            "inbound" => ConfigError::InvalidInbound(format!(
                "`shadowsocks` {kind} cipher `{cipher}` is not valid; expected one of: {}",
                VALID_CIPHERS.join(", ")
            )),
            _ => ConfigError::InvalidOutbound(format!(
                "`shadowsocks` {kind} cipher `{cipher}` is not valid; expected one of: {}",
                VALID_CIPHERS.join(", ")
            )),
        });
    }
    Ok(())
}

fn validate_optional_positive(name: &str, value: Option<u64>) -> Result<(), ConfigError> {
    if value == Some(0) {
        return Err(ConfigError::InvalidOutbound(format!(
            "`{name}` must be greater than 0"
        )));
    }
    Ok(())
}

fn validate_optional_mux_concurrency(name: &str, value: Option<u32>) -> Result<(), ConfigError> {
    validate_optional_positive(name, value.map(u64::from))?;
    if value.is_some_and(|value| value > u16::MAX as u32) {
        return Err(ConfigError::InvalidOutbound(format!(
            "`{name}` must not exceed {}",
            u16::MAX
        )));
    }
    Ok(())
}

fn validate_mux_response_backlog(
    name: &str,
    frames: Option<u32>,
    bytes: Option<u64>,
    validate: fn(Option<u32>, Option<u64>) -> Result<(), &'static str>,
) -> Result<(), ConfigError> {
    let invalid = |message: String| {
        if name.contains("inbound") {
            ConfigError::InvalidInbound(message)
        } else {
            ConfigError::InvalidOutbound(message)
        }
    };

    validate(frames, bytes).map_err(|error| invalid(format!("`{name}` {error}")))
}

fn validate_shadowsocks_password(
    kind: &'static str,
    cipher: &str,
    password: &str,
) -> Result<(), ConfigError> {
    shadowsocks::validation::validate_password(cipher, password).map_err(|error| {
        shadowsocks_password_error(
            kind,
            format!("`shadowsocks` {kind} password is invalid: {error}"),
        )
    })
}

fn shadowsocks_password_error(kind: &'static str, message: String) -> ConfigError {
    match kind {
        "inbound" => ConfigError::InvalidInbound(message),
        _ => ConfigError::InvalidOutbound(message),
    }
}
