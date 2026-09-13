pub(super) fn settings(config: &zero_config::MkcpConfig) -> zero_transport::mkcp::Settings {
    zero_transport::mkcp::Settings {
        mtu: config.mtu,
        tti_ms: config.tti_ms,
        uplink_capacity_mib: config.uplink_capacity_mib,
        downlink_capacity_mib: config.downlink_capacity_mib,
        congestion: config.congestion,
        write_buffer_bytes: config.write_buffer_bytes,
    }
}
