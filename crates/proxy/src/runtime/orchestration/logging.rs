use tracing::info;

use crate::runtime::Proxy;

pub(super) fn log_started(proxy: &Proxy, config: &zero_config::RuntimeConfig) {
    info!(
        inbound_count = config.inbounds.len(),
        outbound_count = config.outbounds.len(),
        outbound_group_count = config.outbound_groups.len(),
        rule_count = config.route.rules.len(),
        mode = %config.mode.kind(),
        udp_upstream_idle_timeout_seconds = proxy.engine.udp_upstream_idle_timeout().as_secs(),
        supported_inbounds = ?proxy.protocols.supported_inbounds(),
        supported_outbounds = ?proxy.protocols.supported_outbounds(),
        "zero-proxy started"
    );
}

pub(super) fn log_reload_reconciled(new_config: &zero_config::RuntimeConfig) {
    info!(
        inbound_count = new_config.inbounds.len(),
        outbound_count = new_config.outbounds.len(),
        outbound_group_count = new_config.outbound_groups.len(),
        "config reload reconciled"
    );
}

pub(super) fn log_stopped(proxy: &Proxy) {
    let stats = proxy.engine.stats_snapshot();
    info!(
        total_started = stats.total_started,
        completed_sessions = stats.completed_sessions,
        failed_sessions = stats.failed_sessions,
        blocked_sessions = stats.blocked_sessions,
        direct_sessions = stats.direct_sessions,
        chained_sessions = stats.chained_sessions,
        udp_upstream_active_associations = stats.udp_upstream.active_associations,
        udp_upstream_created_associations = stats.udp_upstream.created_associations,
        udp_upstream_reused_associations = stats.udp_upstream.reused_associations,
        udp_upstream_closed_associations = stats.udp_upstream.closed_associations,
        udp_upstream_idle_timeouts = stats.udp_upstream.idle_timeouts,
        udp_upstream_dropped_associations = stats.udp_upstream.dropped_associations,
        "zero-proxy stopped"
    );
}
