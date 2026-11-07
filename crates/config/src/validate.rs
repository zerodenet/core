use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use crate::{ConfigError, EventSinkConfig, ModeConfig, RuntimeConfig, RuntimeOptionsConfig};

mod api;
mod control_messages;
mod dial;
mod dns;
mod fallback;
mod group;
mod listeners;
mod protocol;
mod reverse;
mod route;
mod xhttp;

use api::validate_api;
use group::validate_group_reference_graph;
use protocol::{validate_inbound_protocol, validate_outbound_protocol};
use route::{validate_route_target_tag, validate_rule_sets};

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schema_version != zero_api::CONFIG_SCHEMA_VERSION {
            return Err(ConfigError::UnsupportedSchemaVersion {
                found: self.schema_version,
                supported: zero_api::CONFIG_SCHEMA_VERSION,
            });
        }
        let mut inbound_tags = HashSet::new();
        let mut inbound_listens = HashSet::new();
        for (i, inbound) in self.inbounds.iter().enumerate() {
            validate_tag("inbound", &inbound.tag, &mut inbound_tags)
                .map_err(|e| ConfigError::InvalidInbound(format!("inbounds[{i}]: {e}")))?;
            listeners::validate_inbound_listen(
                &mut inbound_listens,
                &inbound.listen.address,
                inbound.listen.port,
            )?;
            validate_inbound_protocol(&inbound.protocol).map_err(|e| {
                ConfigError::InvalidInbound(format!("inbounds[{i}] `{}`: {e}", inbound.tag))
            })?;
        }
        listeners::validate_auxiliary_listeners(self)?;
        if let Some(tun) = &self.runtime.tun {
            validate_tag("TUN inbound", &tun.tag, &mut inbound_tags)
                .map_err(|error| ConfigError::InvalidRuntime(error.to_string()))?;
        }

        let mut outbound_tags = HashSet::new();
        let mut route_target_tags = HashSet::new();
        for (i, outbound) in self.outbounds.iter().enumerate() {
            validate_tag("outbound", &outbound.tag, &mut outbound_tags)
                .map_err(|e| ConfigError::InvalidOutbound(format!("outbounds[{i}]: {e}")))?;
            validate_outbound_protocol(&outbound.protocol).map_err(|e| {
                ConfigError::InvalidOutbound(format!("outbounds[{i}] `{}`: {e}", outbound.tag))
            })?;
            dial::validate_outbound_dial(outbound).map_err(|e| {
                ConfigError::InvalidOutbound(format!("outbounds[{i}] `{}`: {e}", outbound.tag))
            })?;
            validate_route_target_tag(outbound.tag(), &mut route_target_tags)?;
        }

        // A linked outbound is another view of the same authenticated device.
        // Reject mismatched keys or peer tables before either side can bind.
        let mut claimed_wireguard_inbounds = HashSet::new();
        for outbound in &self.outbounds {
            let crate::OutboundProtocolConfig::Wireguard {
                private_key,
                addresses,
                mtu,
                peers,
                inbound_tag: Some(inbound_tag),
                ..
            } = &outbound.protocol
            else {
                continue;
            };
            if !claimed_wireguard_inbounds.insert(inbound_tag) {
                return Err(ConfigError::InvalidOutbound(format!(
                    "wireguard inbound `{inbound_tag}` is linked by multiple outbounds"
                )));
            }
            let inbound = self
                .inbounds
                .iter()
                .find(|inbound| inbound.tag == *inbound_tag)
                .ok_or_else(|| {
                    ConfigError::InvalidOutbound(format!(
                        "wireguard outbound `{}` refers to missing inbound `{inbound_tag}`",
                        outbound.tag
                    ))
                })?;
            let crate::InboundProtocolConfig::Wireguard {
                private_key: inbound_key,
                addresses: inbound_addresses,
                mtu: inbound_mtu,
                peers: inbound_peers,
                ..
            } = &inbound.protocol
            else {
                return Err(ConfigError::InvalidOutbound(format!(
                    "wireguard outbound `{}` refers to a non-WireGuard inbound",
                    outbound.tag
                )));
            };
            if private_key != inbound_key
                || (!inbound_addresses.is_empty() && addresses != inbound_addresses)
                || mtu != inbound_mtu
                || peers.len() != inbound_peers.len()
                || peers.iter().zip(inbound_peers).any(|(a, b)| {
                    a.public_key != b.public_key
                        || a.pre_shared_key != b.pre_shared_key
                        || a.allowed_ips != b.allowed_ips
                        || a.keepalive_secs != b.keepalive_secs
                        || a.reserved != b.reserved
                })
            {
                return Err(ConfigError::InvalidOutbound(format!(
                    "wireguard outbound `{}` must use the linked inbound's key, addresses, MTU and ordered peers",
                    outbound.tag
                )));
            }
        }

        reverse::validate(self, &mut inbound_tags)?;

        let mut outbound_group_tags = HashSet::new();
        for (i, group) in self.outbound_groups.iter().enumerate() {
            validate_tag("outbound group", &group.tag, &mut outbound_group_tags).map_err(|e| {
                ConfigError::InvalidOutboundGroup(format!("outbound_groups[{i}]: {e}"))
            })?;
            validate_route_target_tag(group.tag(), &mut route_target_tags)?;
        }

        let mut group_target_tags = outbound_tags.clone();
        group_target_tags.extend(outbound_group_tags.iter().cloned());

        for group in &self.outbound_groups {
            group.validate(&group_target_tags)?;
        }
        validate_group_reference_graph(&self.outbound_groups)?;

        for outbound in &self.outbounds {
            let crate::OutboundProtocolConfig::Wireguard {
                outer_udp_proxy: Some(proxy_tag),
                ..
            } = &outbound.protocol
            else {
                continue;
            };
            if proxy_tag == &outbound.tag {
                return Err(ConfigError::InvalidOutbound(format!(
                    "wireguard outbound `{}` cannot use itself as outer_udp_proxy",
                    outbound.tag
                )));
            }
            if let Some(group) = self
                .outbound_groups
                .iter()
                .find(|group| group.tag == *proxy_tag)
            {
                let crate::OutboundGroupKind::Relay { proxies } = &group.group else {
                    return Err(ConfigError::InvalidOutbound(format!(
                        "wireguard outbound `{}` outer_udp_proxy `{proxy_tag}` must be a relay group",
                        outbound.tag
                    )));
                };
                if proxies.iter().any(|member| member == &outbound.tag) {
                    return Err(ConfigError::InvalidOutbound(format!(
                        "wireguard outbound `{}` cannot occur in its outer_udp_proxy relay group `{proxy_tag}`",
                        outbound.tag
                    )));
                }
                if proxies.iter().any(|member| !outbound_tags.contains(member)) {
                    return Err(ConfigError::InvalidOutbound(format!(
                        "wireguard outbound `{}` outer_udp_proxy relay group `{proxy_tag}` must contain only concrete outbounds",
                        outbound.tag
                    )));
                }
            } else if !outbound_tags.contains(proxy_tag) {
                return Err(ConfigError::InvalidOutbound(format!(
                    "wireguard outbound `{}` references unknown outer_udp_proxy `{proxy_tag}`",
                    outbound.tag
                )));
            }
        }

        let rule_set_tags = validate_rule_sets(&self.route.rule_sets)?;
        self.route
            .validate(&route_target_tags, &inbound_tags, &rule_set_tags)?;
        validate_runtime(
            &self.runtime,
            &self.route.rule_sets,
            &rule_set_tags,
            &route_target_tags,
        )?;
        validate_mode(&self.mode, &route_target_tags)?;
        validate_api(&self.api)?;
        validate_connector_state_paths(self)?;
        self.automatic_route_prefixes()?;

        Ok(())
    }
}

fn validate_connector_state_paths(config: &RuntimeConfig) -> Result<(), ConfigError> {
    let mut paths = Vec::new();
    if let Some(path) = config.runtime.principal_quota_state_path.as_deref() {
        paths.push(("runtime.principal_quota_state_path".to_owned(), path, true));
    }
    if let Some(path) = config.api.outbox_path.as_deref() {
        paths.push(("api.outbox_path".to_owned(), path, true));
    }
    if let Some(path) = config.api.dead_letter_path.as_deref() {
        paths.push(("api.dead_letter_path".to_owned(), path, true));
    }
    for (index, sink) in config.api.event_sinks.iter().enumerate() {
        if let EventSinkConfig::JsonLines { path, .. } = sink {
            paths.push((
                format!("api.event_sinks[{index}].path"),
                path.as_str(),
                true,
            ));
        }
    }
    let mut owners = HashMap::new();
    let mut normalized_paths = Vec::new();
    for (field, path, leased) in paths {
        let normalized = normalize_connector_state_path(path, config.source_dir());
        let key = connector_state_path_key(&normalized);
        if let Some(other_field) = owners.insert(key, field.clone()) {
            return Err(ConfigError::InvalidApi(format!(
                "{field} must not share a file with {other_field}"
            )));
        }
        normalized_paths.push((field, normalized, leased));
    }
    for (field, path, leased) in &normalized_paths {
        if !leased {
            continue;
        }
        let lock_path = connector_state_lock_path(path);
        let lock_key = connector_state_path_key(&lock_path);
        if let Some(other_field) = owners.get(&lock_key) {
            return Err(ConfigError::InvalidApi(format!(
                "{other_field} must not use the lock file reserved for {field}"
            )));
        }
    }
    Ok(())
}

fn normalize_connector_state_path(path: &str, source_dir: Option<&Path>) -> PathBuf {
    let path = PathBuf::from(path);
    let resolved = if path.is_absolute() {
        path
    } else {
        let source_dir = source_dir.unwrap_or_else(|| Path::new("."));
        let base = if source_dir.is_absolute() {
            source_dir.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(source_dir)
        };
        base.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in resolved.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if normalized.file_name().is_some() {
                    normalized.pop();
                } else if !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(windows)]
fn connector_state_path_key(path: &Path) -> String {
    path.to_string_lossy().to_lowercase()
}

#[cfg(not(windows))]
fn connector_state_path_key(path: &Path) -> PathBuf {
    path.to_path_buf()
}

fn connector_state_lock_path(path: &Path) -> PathBuf {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".zero.lock");
    PathBuf::from(lock_path)
}

pub(crate) fn validate_tag(
    scope: &'static str,
    tag: &str,
    seen: &mut HashSet<String>,
) -> Result<(), ConfigError> {
    if tag.trim().is_empty() {
        return Err(ConfigError::EmptyTag { scope });
    }

    if !seen.insert(tag.to_owned()) {
        return Err(ConfigError::DuplicateTag {
            scope,
            tag: tag.to_owned(),
        });
    }

    Ok(())
}

fn validate_mode(
    mode: &ModeConfig,
    route_target_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    match mode {
        ModeConfig::Rule | ModeConfig::Direct => Ok(()),
        ModeConfig::Global { outbound } => {
            if outbound.trim().is_empty() {
                return Err(ConfigError::InvalidMode(
                    "`global` mode requires a non-empty outbound target".to_owned(),
                ));
            }

            if !route_target_tags.contains(outbound) {
                return Err(ConfigError::UndefinedRouteTargetTag {
                    tag: outbound.to_owned(),
                });
            }

            Ok(())
        }
    }
}

fn validate_runtime(
    runtime: &RuntimeOptionsConfig,
    rule_sets: &[crate::RouteRuleSetConfig],
    rule_set_tags: &HashSet<String>,
    route_target_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    if runtime
        .principal_quota_state_path
        .as_deref()
        .is_some_and(|path| path.trim().is_empty())
    {
        return Err(ConfigError::InvalidRuntime(
            "runtime.principal_quota_state_path must not be empty".to_owned(),
        ));
    }
    if runtime.udp_upstream_idle_timeout_seconds == 0 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.udp_upstream_idle_timeout_seconds` must be greater than 0".to_owned(),
        ));
    }
    if runtime.effective_file_read_max_bytes() == 0
        || runtime
            .effective_file_read_max_bytes()
            .checked_add(1)
            .is_none()
    {
        return Err(ConfigError::InvalidRuntime(
            "runtime.file_read_max_bytes must be positive and leave room for the overflow probe"
                .into(),
        ));
    }
    if runtime.effective_native_mux_max_carriers() == 0
        || runtime.effective_native_mux_max_carriers() > 4096
    {
        return Err(ConfigError::InvalidRuntime(
            "runtime.native_mux_max_carriers must be between 1 and 4096".into(),
        ));
    }
    if !(1..=4096).contains(&runtime.effective_udp_direct_max_sockets()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.udp_direct_max_sockets must be between 1 and 4096".into(),
        ));
    }
    if !(1..=65536).contains(&runtime.effective_packet_return_max_frames()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_return_max_frames must be between 1 and 65536".into(),
        ));
    }
    if !(1..=1073741824).contains(&runtime.effective_packet_return_max_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_return_max_bytes must be between 1 and 1073741824".into(),
        ));
    }
    if !(1..=65536).contains(&runtime.effective_packet_return_total_max_frames()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_return_total_max_frames must be between 1 and 65536".into(),
        ));
    }
    if !(1..=1073741824).contains(&runtime.effective_packet_return_total_max_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_return_total_max_bytes must be between 1 and 1073741824".into(),
        ));
    }
    if !(1..=65536).contains(&runtime.effective_packet_transmit_total_max_frames()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_transmit_total_max_frames must be between 1 and 65536".into(),
        ));
    }
    if !(1..=1073741824).contains(&runtime.effective_packet_transmit_total_max_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_transmit_total_max_bytes must be between 1 and 1073741824".into(),
        ));
    }
    if !(1..=65536).contains(&runtime.effective_packet_native_read_total_max_frames()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_native_read_total_max_frames must be between 1 and 65536".into(),
        ));
    }
    if !(1..=1073741824).contains(&runtime.effective_packet_native_read_total_max_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.packet_native_read_total_max_bytes must be between 1 and 1073741824".into(),
        ));
    }
    for (name, limit) in [
        (
            "ss_udp_replay_max_windows",
            runtime.effective_ss_udp_replay_max_windows(),
        ),
        ("ss_udp_max_senders", runtime.effective_ss_udp_max_senders()),
        (
            "ss_udp_max_associations",
            runtime.effective_ss_udp_max_associations(),
        ),
        (
            "ss_udp_max_return_bindings",
            runtime.effective_ss_udp_max_return_bindings(),
        ),
        (
            "ss_udp_max_authenticated_codecs",
            runtime.effective_ss_udp_max_authenticated_codecs(),
        ),
    ] {
        if limit == 0 || limit.checked_add(1).is_none() {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be positive and leave room for checked accounting"
            )));
        }
    }
    if !(1..=4096).contains(&runtime.effective_grpc_pool_max_carriers()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.grpc_pool_max_carriers must be between 1 and 4096".into(),
        ));
    }
    if !(1..=65536).contains(&runtime.effective_grpc_pool_max_streams()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.grpc_pool_max_streams must be between 1 and 65536".into(),
        ));
    }
    if !(1..=4096).contains(&runtime.effective_grpc_total_max_carriers()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.grpc_total_max_carriers must be between 1 and 4096".into(),
        ));
    }
    for (name, value, maximum) in [
        (
            "navigation_max_jobs",
            runtime.effective_navigation_max_jobs(),
            64,
        ),
        (
            "navigation_max_requests",
            runtime.effective_navigation_max_requests(),
            32,
        ),
        (
            "packet_route_max_routes",
            runtime.effective_packet_route_max_routes(),
            4096,
        ),
    ] {
        if !(1..=maximum).contains(&value) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and {maximum}"
            )));
        }
    }
    if !(1..=65536).contains(&runtime.effective_grpc_total_max_streams()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.grpc_total_max_streams must be between 1 and 65536".into(),
        ));
    }
    if !(1..=8).contains(&runtime.effective_diagnostic_max_operations()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.diagnostic_max_operations must be between 1 and 8".into(),
        ));
    }
    if runtime
        .system_tun_task_limit
        .is_some_and(|limit| !(1..=65536).contains(&limit))
    {
        return Err(ConfigError::InvalidRuntime(
            "runtime.system_tun_task_limit must be between 1 and 65536".into(),
        ));
    }
    if runtime.effective_ss_legacy_replay_max_bytes() == 0
        || runtime
            .effective_ss_legacy_replay_max_bytes()
            .checked_add(1)
            .is_none()
    {
        return Err(ConfigError::InvalidRuntime("runtime.ss_legacy_replay_max_bytes must be positive and leave room for checked accounting".into()));
    }
    if runtime.effective_ss_tcp_replay_max_salts() == 0
        || runtime
            .effective_ss_tcp_replay_max_salts()
            .checked_add(1)
            .is_none()
    {
        return Err(ConfigError::InvalidRuntime(
            "runtime.ss_tcp_replay_max_salts must be positive and leave room for checked accounting".into(),
        ));
    }
    if !(1..=4096).contains(&runtime.effective_vmess_grpc_max_originals()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.vmess_grpc_max_originals must be between 1 and 4096".into(),
        ));
    }
    if !(1..=256).contains(&runtime.effective_mieru_max_carriers()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.mieru_max_carriers must be between 1 and 256".into(),
        ));
    }
    if !(1..=256).contains(&runtime.effective_hysteria2_max_entries()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.hysteria2_max_entries must be between 1 and 256".into(),
        ));
    }
    if !(1..=4096).contains(&runtime.effective_hysteria2_max_udp_sessions()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.hysteria2_max_udp_sessions must be between 1 and 4096".into(),
        ));
    }
    if !(1..=64 * 1024 * 1024).contains(&runtime.effective_hysteria2_udp_pending_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.hysteria2_udp_pending_bytes must be between 1 and 67108864".into(),
        ));
    }
    for (name, limit) in [
        (
            "ss_udp_max_pending_responses",
            runtime.effective_ss_udp_max_pending_responses(),
        ),
        (
            "ss_udp_total_max_pending_responses",
            runtime.effective_ss_udp_total_max_pending_responses(),
        ),
    ] {
        if !(1..=65536).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 65536"
            )));
        }
    }
    if !(1..=65536).contains(&runtime.effective_carrier_max_total_originals()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.carrier_max_total_originals must be between 1 and 65536".into(),
        ));
    }
    if !(1..=4096).contains(&runtime.effective_native_stream_max_originals()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.native_stream_max_originals must be between 1 and 4096".into(),
        ));
    }
    for (name, limit) in [
        (
            "direct_max_originals",
            runtime.effective_direct_max_originals(),
        ),
        ("direct_max_tasks", runtime.effective_direct_max_tasks()),
    ] {
        if !(1..=4096).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 4096"
            )));
        }
    }
    if !(1..=4096).contains(&runtime.effective_process_max_jobs())
        || !(1..=runtime.effective_process_max_jobs())
            .contains(&runtime.effective_process_max_jobs_per_instance())
    {
        return Err(ConfigError::InvalidRuntime("runtime process job limits must be positive, per-instance no greater than total, and total at most 4096".into()));
    }
    for (name, limit) in [
        (
            "control_max_connections",
            runtime.effective_control_max_connections(),
        ),
        (
            "control_max_subscriptions",
            runtime.effective_control_max_subscriptions(),
        ),
        (
            "event_network_max_tasks",
            runtime.effective_event_network_max_tasks(),
        ),
        (
            "event_network_max_originals",
            runtime.effective_event_network_max_originals(),
        ),
        (
            "event_network_max_lookups",
            runtime.effective_event_network_max_lookups(),
        ),
    ] {
        if !(1..=4096).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 4096"
            )));
        }
    }
    control_messages::validate(runtime)?;
    if !(1..=256 * 1024 * 1024).contains(&runtime.effective_control_pipe_max_workspace_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.control_pipe_max_workspace_bytes must be between 1 and 268435456".into(),
        ));
    }
    if !(1024..=64 * 1024 * 1024).contains(&runtime.effective_control_max_queued_event_bytes()) {
        return Err(ConfigError::InvalidRuntime(
            "runtime.control_max_queued_event_bytes must be between 1024 and 67108864".into(),
        ));
    }
    for (name, limit) in [
        (
            "native_dns_max_originals",
            runtime.effective_native_dns_max_originals(),
        ),
        (
            "native_dns_max_queries",
            runtime.effective_native_dns_max_queries(),
        ),
        (
            "native_dns_max_blocking_jobs",
            runtime.effective_native_dns_max_blocking_jobs(),
        ),
        (
            "native_dns_max_h2_drivers",
            runtime.effective_native_dns_max_h2_drivers(),
        ),
    ] {
        if !(1..=4096).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 4096"
            )));
        }
    }
    // Actual subscriber queue/count/filter books, independent of replay history.
    for (name, limit, maximum) in [
        (
            "event_subscriber_max_pending_events",
            runtime.effective_event_subscriber_max_pending_events(),
            1024,
        ),
        (
            "event_subscriber_total_max_pending_events",
            runtime.effective_event_subscriber_total_max_pending_events(),
            1048576,
        ),
        (
            "event_subscriber_max_registrations",
            runtime.effective_event_subscriber_max_registrations(),
            65536,
        ),
        (
            "event_subscriber_total_stats_max_bytes",
            runtime.effective_event_subscriber_total_stats_max_bytes(),
            268435456,
        ),
        (
            "event_subscriber_max_filter_bytes",
            runtime.effective_event_subscriber_max_filter_bytes(),
            1048576,
        ),
        (
            "event_subscriber_total_max_storage_bytes",
            runtime.effective_event_subscriber_total_max_storage_bytes(),
            268435456,
        ),
    ] {
        if !(1..=maximum).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and {maximum}"
            )));
        }
    }
    for (name, limit) in [
        (
            "event_log_stats_max_bytes",
            runtime.effective_event_log_stats_max_bytes(),
        ),
        (
            "event_subscriber_stats_max_bytes",
            runtime.effective_event_subscriber_stats_max_bytes(),
        ),
    ] {
        if !(1..=256 * 1024 * 1024).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 268435456"
            )));
        }
    }
    for (name, limit, maximum) in [
        (
            "inbound_max_accepted_sockets",
            runtime.effective_inbound_max_accepted_sockets(),
            65536,
        ),
        (
            "inbound_max_datagram_associations",
            runtime.effective_inbound_max_datagram_associations(),
            65536,
        ),
        (
            "inbound_max_datagram_peers",
            runtime.effective_inbound_max_datagram_peers(128),
            65536,
        ),
        (
            "inbound_max_packets_per_peer",
            runtime.effective_inbound_max_packets_per_peer(256),
            65536,
        ),
        (
            "inbound_max_pending_sends",
            runtime.effective_inbound_max_pending_sends(32),
            65536,
        ),
        (
            "inbound_max_bind_attempts",
            runtime.effective_inbound_max_bind_attempts(),
            4096,
        ),
        (
            "inbound_max_target_sockets",
            runtime.effective_inbound_max_target_sockets(),
            65536,
        ),
        (
            "inbound_max_file_sessions",
            runtime.effective_inbound_max_file_sessions(),
            4096,
        ),
        (
            "inbound_max_handoff_probe_attempts",
            runtime.effective_inbound_max_handoff_probe_attempts(),
            65536,
        ),
        (
            "metadata_max_probe_sockets",
            runtime.effective_metadata_max_probe_sockets(2),
            256,
        ),
        (
            "metadata_max_probe_attempts",
            runtime.effective_metadata_max_probe_attempts(8),
            4096,
        ),
        (
            "metadata_max_send_calls",
            runtime.effective_metadata_max_send_calls(2),
            4096,
        ),
        (
            "quic_source_max_connections",
            runtime.effective_quic_source_max_connections(),
            65536,
        ),
        (
            "quic_shared_endpoint_max_users",
            runtime.effective_quic_shared_endpoint_max_users(),
            65536,
        ),
        (
            "quic_max_queue_packets",
            runtime.effective_quic_max_queue_packets(),
            256,
        ),
        (
            "quic_total_max_buffered_bytes",
            runtime.effective_quic_total_max_buffered_bytes(),
            1_073_741_824,
        ),
    ] {
        if !(1..=maximum).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and {maximum}"
            )));
        }
    }
    for (name, limit, minimum, maximum) in [
        (
            "inbound_max_tasks_per_carrier",
            runtime.effective_inbound_max_tasks_per_carrier(520),
            1,
            4096,
        ),
        (
            "inbound_max_resources_per_carrier",
            runtime.effective_inbound_max_resources_per_carrier(257),
            0,
            65536,
        ),
        (
            "inbound_max_in_flight_requests",
            runtime.effective_inbound_max_in_flight_requests(256),
            1,
            65536,
        ),
    ] {
        if !(minimum..=maximum).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between {minimum} and {maximum}"
            )));
        }
    }
    for (name, limit) in [
        (
            "hysteria2_h3_max_pending_classifications",
            runtime.effective_hysteria2_h3_max_pending_classifications(),
        ),
        (
            "hysteria2_h3_max_pending_tcp_streams",
            runtime.effective_hysteria2_h3_max_pending_tcp_streams(),
        ),
        (
            "hysteria2_h3_max_pending_web_streams",
            runtime.effective_hysteria2_h3_max_pending_web_streams(),
        ),
        (
            "hysteria2_h3_max_pending_web_requests",
            runtime.effective_hysteria2_h3_max_pending_web_requests(),
        ),
    ] {
        if !(1..=4096).contains(&limit) {
            return Err(ConfigError::InvalidRuntime(format!(
                "runtime.{name} must be between 1 and 4096"
            )));
        }
    }
    if runtime.event_log_capacity == 0 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.event_log_capacity` must be greater than 0".to_owned(),
        ));
    }

    runtime.network.udp_receive_queue.validate()?;

    if let Some(url) = &runtime.latency_test_url {
        validate_latency_test_url("`runtime.latency_test_url`", url)?;
    }

    if let Some(device) = &runtime.network.direct_packet_device {
        if device.write_queue.get() > 4096 {
            return Err(ConfigError::InvalidRuntime(
                "runtime.network.direct_packet_device.write_queue must be between 1 and 4096"
                    .into(),
            ));
        }
        let binding_valid = match device.backend {
            crate::DirectPacketDeviceBackend::Descriptor => device.fd.is_some_and(|fd| fd >= 3),
            crate::DirectPacketDeviceBackend::Wintun => device.fd.is_none(),
        };
        let name_valid = match device.backend {
            crate::DirectPacketDeviceBackend::Descriptor => device.interface.len() <= 15,
            crate::DirectPacketDeviceBackend::Wintun => {
                device.interface.encode_utf16().count() <= 128
            }
        };
        if !binding_valid
            || device.interface.is_empty()
            || !name_valid
            || device.interface.contains('\0')
            || device.router_addresses.is_empty()
            || device.router_addresses.len() > 2
            || device
                .router_addresses
                .iter()
                .any(|ip| ip.is_unspecified() || ip.is_multicast())
            || device
                .router_addresses
                .windows(2)
                .any(|ips| ips[0].is_ipv4() == ips[1].is_ipv4())
        {
            return Err(ConfigError::InvalidRuntime(
                "Direct packet device requires descriptor fd >= 3 or wintun without fd, a valid interface name and one unicast router address per supplied family".into(),
            ));
        }
        if runtime
            .tun
            .as_ref()
            .is_some_and(|tun| match device.backend {
                crate::DirectPacketDeviceBackend::Descriptor => {
                    tun.name.as_ref() == Some(&device.interface)
                }
                crate::DirectPacketDeviceBackend::Wintun => tun
                    .name
                    .as_deref()
                    .unwrap_or("ZeroTun")
                    .eq_ignore_ascii_case(&device.interface),
            })
        {
            return Err(ConfigError::InvalidRuntime(
                "Direct packet device must be separate from the ingress TUN".into(),
            ));
        }
    }
    if runtime.network.mtu < 576 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.network.mtu` must be at least 576".to_owned(),
        ));
    }

    if let Some(tun) = &runtime.tun {
        validate_tun_config(tun, runtime)?;
    }

    if let Some(dns) = &runtime.dns {
        dns::validate_dns_config(runtime, dns, rule_sets, rule_set_tags, route_target_tags)?;
    }

    Ok(())
}

fn validate_tun_config(
    tun: &crate::TunConfig,
    runtime: &RuntimeOptionsConfig,
) -> Result<(), ConfigError> {
    if tun
        .name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.tun.name` must not be empty".to_owned(),
        ));
    }
    let (address, prefix) = match tun.addr.split_once('/') {
        Some((address, prefix)) => {
            let address = address.parse::<std::net::IpAddr>().map_err(|error| {
                ConfigError::InvalidRuntime(format!(
                    "invalid `runtime.tun.addr` address `{address}`: {error}"
                ))
            })?;
            let prefix = prefix.parse::<u8>().map_err(|error| {
                ConfigError::InvalidRuntime(format!(
                    "invalid `runtime.tun.addr` prefix `{prefix}`: {error}"
                ))
            })?;
            (address, Some(prefix))
        }
        None => {
            let address = tun.addr.parse::<std::net::IpAddr>().map_err(|error| {
                ConfigError::InvalidRuntime(format!(
                    "invalid `runtime.tun.addr` `{}`: {error}",
                    tun.addr
                ))
            })?;
            (address, None)
        }
    };
    if let Some(prefix) = prefix {
        let maximum = if address.is_ipv4() { 32 } else { 128 };
        if prefix > maximum {
            return Err(ConfigError::InvalidRuntime(format!(
                "invalid `runtime.tun.addr` prefix `{prefix}` for {address}"
            )));
        }
    } else {
        let mask = tun.mask.parse::<std::net::IpAddr>().map_err(|error| {
            ConfigError::InvalidRuntime(format!(
                "invalid `runtime.tun.mask` `{}`: {error}",
                tun.mask
            ))
        })?;
        if address.is_ipv4() != mask.is_ipv4() {
            return Err(ConfigError::InvalidRuntime(
                "`runtime.tun.addr` and `runtime.tun.mask` must use the same address family"
                    .to_owned(),
            ));
        }
        if !is_contiguous_mask(mask) {
            return Err(ConfigError::InvalidRuntime(
                "`runtime.tun.mask` must be a contiguous network mask".to_owned(),
            ));
        }
    }
    if let Some(secondary) = tun.secondary_addr.as_deref() {
        if !tun.dual_stack {
            return Err(ConfigError::InvalidRuntime(
                "`runtime.tun.secondary_addr` requires `runtime.tun.dual_stack=true`".to_owned(),
            ));
        }
        let (secondary_address, secondary_prefix) = secondary.split_once('/').ok_or_else(|| {
            ConfigError::InvalidRuntime(
                "`runtime.tun.secondary_addr` must use CIDR notation".to_owned(),
            )
        })?;
        let secondary_address = secondary_address
            .parse::<std::net::IpAddr>()
            .map_err(|error| {
                ConfigError::InvalidRuntime(format!(
                    "invalid `runtime.tun.secondary_addr` address `{secondary_address}`: {error}"
                ))
            })?;
        let secondary_prefix = secondary_prefix.parse::<u8>().map_err(|error| {
            ConfigError::InvalidRuntime(format!(
                "invalid `runtime.tun.secondary_addr` prefix `{secondary_prefix}`: {error}"
            ))
        })?;
        let maximum = if secondary_address.is_ipv4() { 32 } else { 128 };
        if secondary_prefix > maximum {
            return Err(ConfigError::InvalidRuntime(format!(
                "invalid `runtime.tun.secondary_addr` prefix `{secondary_prefix}` for {secondary_address}"
            )));
        }
        if secondary_address.is_ipv4() == address.is_ipv4() {
            return Err(ConfigError::InvalidRuntime(
                "`runtime.tun.addr` and `runtime.tun.secondary_addr` must use different address families"
                    .to_owned(),
            ));
        }
    }
    if tun.effective_mtu(runtime.network.mtu) < 576 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.tun.mtu` must be at least 576".to_owned(),
        ));
    }
    if (!tun.include_cidrs.is_empty() || !tun.exclude_cidrs.is_empty()) && !tun.auto_route {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.tun.include_cidrs` and `runtime.tun.exclude_cidrs` require `runtime.tun.auto_route=true`".to_owned(),
        ));
    }
    if tun.include_cidrs.len() > 128 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.tun.include_cidrs` supports at most 128 entries".to_owned(),
        ));
    }
    let mut seen_cidrs = HashSet::new();
    for cidr in &tun.include_cidrs {
        if !seen_cidrs.insert(*cidr) {
            return Err(ConfigError::InvalidRuntime(format!(
                "duplicate `runtime.tun.include_cidrs` entry `{cidr}`"
            )));
        }
        if !tun.dual_stack && cidr.addr().is_ipv4() != address.is_ipv4() {
            return Err(ConfigError::InvalidRuntime(format!(
                "`runtime.tun.include_cidrs` entry `{cidr}` has no configured TUN address family"
            )));
        }
    }
    if tun.exclude_cidrs.len() > 128 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.tun.exclude_cidrs` supports at most 128 entries".to_owned(),
        ));
    }
    let mut seen_cidrs = HashSet::new();
    for cidr in &tun.exclude_cidrs {
        if !seen_cidrs.insert(*cidr) {
            return Err(ConfigError::InvalidRuntime(format!(
                "duplicate `runtime.tun.exclude_cidrs` entry `{cidr}`"
            )));
        }
        if !tun.dual_stack && cidr.addr().is_ipv4() != address.is_ipv4() {
            return Err(ConfigError::InvalidRuntime(format!(
                "`runtime.tun.exclude_cidrs` entry `{cidr}` has no configured TUN address family"
            )));
        }
    }
    if tun.dns_hijack && tun.strict_route {
        runtime
            .dns
            .as_ref()
            .ok_or_else(|| {
                ConfigError::InvalidRuntime(
                    "TUN DNS hijack requires a configured DNS server".to_owned(),
                )
            })?
            .tun_route_exclusion_addresses()
            .map_err(ConfigError::InvalidRuntime)?;
    }
    Ok(())
}

fn is_contiguous_mask(mask: std::net::IpAddr) -> bool {
    let bits = match mask {
        std::net::IpAddr::V4(mask) => u32::from(mask) as u128,
        std::net::IpAddr::V6(mask) => u128::from(mask),
    };
    let width = if mask.is_ipv4() { 32 } else { 128 };
    let relevant = if width == 32 { bits << 96 } else { bits };
    let inverted = !relevant;
    inverted == 0 || (inverted & inverted.wrapping_add(1)) == 0
}

pub(crate) fn validate_latency_test_url(scope: &str, url: &str) -> Result<(), ConfigError> {
    if url.trim().is_empty() {
        return Err(ConfigError::InvalidRuntime(format!(
            "{scope} must not be empty"
        )));
    }
    if !url.starts_with("http://") {
        return Err(ConfigError::InvalidRuntime(format!(
            "{scope} currently only supports `http://` URLs"
        )));
    }
    Ok(())
}
