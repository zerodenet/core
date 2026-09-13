//! Thin config projection; carrier validation and execution stay in transport.
pub(super) fn options(
    config: &zero_config::HysteriaTransportConfig,
) -> zero_transport::hysteria::OptionsRef<'_> {
    let q = &config.quic_parameters;
    zero_transport::hysteria::OptionsRef {
        auth: &config.auth,
        congestion: &config.congestion,
        uplink_bytes_per_sec: config.uplink_bytes_per_sec,
        downlink_bytes_per_sec: config.downlink_bytes_per_sec,
        udp_hop: q
            .udp_hop
            .as_ref()
            .map(|hop| zero_transport::datagram_hop::OptionsRef {
                ports: &hop.ports,
                interval_min_secs: hop.interval_min_secs,
                interval_max_secs: hop.interval_max_secs,
            }),
        quic: zero_transport::hysteria::Parameters {
            initial_stream_receive_window: q.initial_stream_receive_window,
            max_stream_receive_window: q.max_stream_receive_window,
            initial_connection_receive_window: q.initial_connection_receive_window,
            max_connection_receive_window: q.max_connection_receive_window,
            max_idle_timeout_secs: q.max_idle_timeout_secs,
            keep_alive_secs: q.keep_alive_secs,
            max_incoming_streams: q.max_incoming_streams,
            disable_path_mtu_discovery: q.disable_path_mtu_discovery,
        },
    }
}

pub(super) fn inbound(
    config: &zero_config::HysteriaTransportConfig,
    base: Option<&std::path::Path>,
) -> std::io::Result<zero_transport::hysteria::Profile> {
    use zero_config::HysteriaCarrierMasqueradeConfig as Config;
    use zero_transport::hysteria::{Masquerade, Profile};
    let masquerade = match &config.masquerade {
        Config::NotFound => Masquerade::default(),
        Config::String {
            content,
            status,
            headers,
        } => Masquerade::content(content, *status, headers)?,
        Config::File { directory } => Masquerade::file(directory, base)?,
        Config::Proxy {
            url,
            rewrite_host,
            insecure,
        } => Masquerade::proxy(url, *rewrite_host, *insecure)?,
    };
    Ok(Profile::from_options(options(config))?.with_masquerade(masquerade))
}
