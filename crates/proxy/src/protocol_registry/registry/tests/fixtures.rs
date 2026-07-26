use zero_config::{InboundProtocolConfig, OutboundProtocolConfig, RuntimeConfig};
use zero_engine::{OutboundIdentity, ResolvedLeafOutbound};

pub(super) fn inbound_protocol_name(config: &InboundProtocolConfig) -> &'static str {
    match config {
        InboundProtocolConfig::Socks5 { .. } => "socks5",
        InboundProtocolConfig::HttpConnect => "http",
        InboundProtocolConfig::Mixed { .. } => "mixed",
        InboundProtocolConfig::Vless { .. } => "vless",
        InboundProtocolConfig::Hysteria2 { .. } => "hysteria2",
        InboundProtocolConfig::Shadowsocks { .. } => "shadowsocks",
        InboundProtocolConfig::Trojan { .. } => "trojan",
        InboundProtocolConfig::Vmess { .. } => "vmess",
        InboundProtocolConfig::Direct { .. } => "direct",
        InboundProtocolConfig::Mieru { .. } => "mieru",
    }
}

pub(super) fn outbound_leaf_name(
    config: &RuntimeConfig,
    leaf: &ResolvedLeafOutbound<'_>,
) -> &'static str {
    match leaf {
        ResolvedLeafOutbound::Direct { .. } => "direct",
        ResolvedLeafOutbound::Block { .. } => "block",
        ResolvedLeafOutbound::Proxy { identity } => config.outbounds[identity.config_index()]
            .protocol
            .protocol_name(),
    }
}

pub(super) fn compiled_in_inbound_configs() -> Vec<InboundProtocolConfig> {
    let mut configs = vec![InboundProtocolConfig::Direct {
        target: None,
        port: None,
    }];

    #[cfg(feature = "socks5")]
    configs.push(InboundProtocolConfig::Socks5 { users: Vec::new() });
    #[cfg(feature = "http")]
    configs.push(InboundProtocolConfig::HttpConnect);
    #[cfg(feature = "mixed")]
    configs.push(InboundProtocolConfig::Mixed {
        socks5_users: Vec::new(),
    });
    #[cfg(feature = "vless")]
    configs.push(InboundProtocolConfig::Vless {
        users: Vec::new(),
        mux_response_backlog_frames: None,
        mux_response_backlog_bytes: None,
        tls: None,
        reality: None,
        ws: None,
        grpc: None,
        h2: None,
        http_upgrade: None,
        fallback: None,
        quic: None,
        split_http: None,
    });
    #[cfg(feature = "hysteria2")]
    configs.push(InboundProtocolConfig::Hysteria2 {
        password: "password".to_string(),
        users: Vec::new(),
        cert_path: None,
        key_path: None,
        up_bps: None,
        down_bps: None,
    });
    #[cfg(feature = "shadowsocks")]
    configs.push(InboundProtocolConfig::Shadowsocks {
        password: "password".to_string(),
        identity_password: None,
        users: Vec::new(),
        cipher: "chacha20-ietf-poly1305".to_string(),
        up_bps: None,
        down_bps: None,
    });
    #[cfg(feature = "trojan")]
    configs.push(InboundProtocolConfig::Trojan {
        password: "password".to_string(),
        users: Vec::new(),
        sni: None,
        tls: None,
        up_bps: None,
        down_bps: None,
    });
    #[cfg(feature = "vmess")]
    configs.push(InboundProtocolConfig::Vmess {
        users: Vec::new(),
        mux_response_backlog_frames: None,
        mux_response_backlog_bytes: None,
        tls: None,
        ws: None,
        grpc: None,
    });
    #[cfg(feature = "mieru")]
    configs.push(InboundProtocolConfig::Mieru { users: Vec::new() });

    configs
}

fn config_with_outbound(tag: &str, protocol: OutboundProtocolConfig) -> RuntimeConfig {
    let mut config = RuntimeConfig::parse(
        r#"{
            "outbounds": [{
                "tag": "placeholder",
                "protocol": { "type": "direct" }
            }],
            "route": { "rules": [], "final": { "type": "direct" } }
        }"#,
    )
    .expect("minimal config");
    config.outbounds[0].tag = tag.to_owned();
    config.outbounds[0].protocol = protocol;
    config
}

fn proxy_leaf() -> ResolvedLeafOutbound<'static> {
    ResolvedLeafOutbound::Proxy {
        identity: OutboundIdentity::from_config_index(0),
    }
}

pub(super) fn compiled_in_outbound_leaves(
) -> Vec<(RuntimeConfig, ResolvedLeafOutbound<'static>, usize)> {
    let minimal = || {
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("minimal config")
    };
    let mut leaves = vec![
        (
            minimal(),
            ResolvedLeafOutbound::Direct {
                tag: Some("direct"),
            },
            1,
        ),
        (
            minimal(),
            ResolvedLeafOutbound::Block { tag: Some("block") },
            0,
        ),
    ];

    #[cfg(feature = "socks5")]
    leaves.push((
        config_with_outbound(
            "socks5",
            OutboundProtocolConfig::Socks5 {
                server: "127.0.0.1".to_owned(),
                port: 1080,
                username: None,
                password: None,
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "vless")]
    leaves.push((
        config_with_outbound(
            "vless",
            OutboundProtocolConfig::Vless {
                server: "127.0.0.1".to_owned(),
                port: 443,
                id: "00000000-0000-0000-0000-000000000000".to_owned(),
                flow: None,
                mux_concurrency: None,
                xudp_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
                tls: None,
                reality: None,
                ws: None,
                grpc: None,
                h2: None,
                http_upgrade: None,
                split_http: None,
                quic: None,
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "hysteria2")]
    leaves.push((
        config_with_outbound(
            "hysteria2",
            OutboundProtocolConfig::Hysteria2 {
                server: "127.0.0.1".to_owned(),
                port: 443,
                password: "password".to_owned(),
                insecure: false,
                client_fingerprint: None,
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "shadowsocks")]
    leaves.push((
        config_with_outbound(
            "shadowsocks",
            OutboundProtocolConfig::Shadowsocks {
                server: "127.0.0.1".to_owned(),
                port: 8388,
                password: "password".to_owned(),
                cipher: "chacha20-ietf-poly1305".to_owned(),
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "trojan")]
    leaves.push((
        config_with_outbound(
            "trojan",
            OutboundProtocolConfig::Trojan {
                server: "127.0.0.1".to_owned(),
                port: 443,
                password: "password".to_owned(),
                sni: None,
                insecure: false,
                client_fingerprint: None,
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "vmess")]
    leaves.push((
        config_with_outbound(
            "vmess",
            OutboundProtocolConfig::Vmess {
                server: "127.0.0.1".to_owned(),
                port: 443,
                id: "00000000-0000-0000-0000-000000000000".to_owned(),
                cipher: "aes-128-gcm".to_owned(),
                mux_concurrency: None,
                mux_idle_timeout_secs: None,
                mux_response_backlog_frames: None,
                mux_response_backlog_bytes: None,
                tls: None,
                ws: None,
                grpc: None,
            },
        ),
        proxy_leaf(),
        1,
    ));
    #[cfg(feature = "mieru")]
    leaves.push((
        config_with_outbound(
            "mieru",
            OutboundProtocolConfig::Mieru {
                server: "127.0.0.1".to_owned(),
                port: 8964,
                username: Some("password".to_owned()),
                password: "password".to_owned(),
            },
        ),
        proxy_leaf(),
        1,
    ));

    leaves
}
