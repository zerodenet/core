use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use crate::{ConfigError, EventSinkConfig, ModeConfig, RuntimeConfig, RuntimeOptionsConfig};

mod api;
mod group;
mod protocol;
mod route;

use api::validate_api;
use group::validate_group_reference_graph;
use protocol::{validate_inbound_protocol, validate_outbound_protocol};
use route::validate_route_target_tag;

impl RuntimeConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut inbound_tags = HashSet::new();
        let mut inbound_listens = HashSet::new();
        for (i, inbound) in self.inbounds.iter().enumerate() {
            validate_tag("inbound", &inbound.tag, &mut inbound_tags)
                .map_err(|e| ConfigError::InvalidInbound(format!("inbounds[{i}]: {e}")))?;
            validate_inbound_listen(
                &mut inbound_listens,
                &inbound.listen.address,
                inbound.listen.port,
            )?;
            validate_inbound_protocol(&inbound.protocol).map_err(|e| {
                ConfigError::InvalidInbound(format!("inbounds[{i}] `{}`: {e}", inbound.tag))
            })?;
        }

        let mut outbound_tags = HashSet::new();
        let mut route_target_tags = HashSet::new();
        for (i, outbound) in self.outbounds.iter().enumerate() {
            validate_tag("outbound", &outbound.tag, &mut outbound_tags)
                .map_err(|e| ConfigError::InvalidOutbound(format!("outbounds[{i}]: {e}")))?;
            validate_outbound_protocol(&outbound.protocol).map_err(|e| {
                ConfigError::InvalidOutbound(format!("outbounds[{i}] `{}`: {e}", outbound.tag))
            })?;
            validate_route_target_tag(outbound.tag(), &mut route_target_tags)?;
        }

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

        self.route
            .validate(&route_target_tags, &inbound_tags, self.source_dir())?;
        validate_runtime(&self.runtime)?;
        validate_mode(&self.mode, &route_target_tags)?;
        validate_api(&self.api)?;
        validate_connector_state_paths(self)?;

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

fn validate_runtime(runtime: &RuntimeOptionsConfig) -> Result<(), ConfigError> {
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
    if runtime.event_log_capacity == 0 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.event_log_capacity` must be greater than 0".to_owned(),
        ));
    }

    if let Some(url) = &runtime.latency_test_url {
        validate_latency_test_url("`runtime.latency_test_url`", url)?;
    }

    if runtime.network.mtu < 576 {
        return Err(ConfigError::InvalidRuntime(
            "`runtime.network.mtu` must be at least 576".to_owned(),
        ));
    }

    if let Some(dns) = &runtime.dns {
        validate_dns_config(dns)?;
    }

    Ok(())
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

fn validate_dns_config(dns: &crate::DnsConfig) -> Result<(), ConfigError> {
    let num_servers = dns.servers.len();
    for (i, server) in dns.servers.iter().enumerate() {
        match server {
            crate::DnsServerConfig::Udp { address, .. } if address.trim().is_empty() => {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server {i}: udp address must not be empty"
                )));
            }
            crate::DnsServerConfig::Dot { address, .. } if address.trim().is_empty() => {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server {i}: dot address must not be empty"
                )));
            }
            crate::DnsServerConfig::Doh { url, .. } if url.trim().is_empty() => {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server {i}: doh url must not be empty"
                )));
            }
            _ => {}
        }
    }

    if let Some(cache) = &dns.cache {
        if cache.max_entries == 0 {
            return Err(ConfigError::InvalidDns(
                "`dns.cache.max_entries` must be greater than 0".to_owned(),
            ));
        }
    }

    if let Some(fake_ip) = &dns.fake_ip {
        let cidr: Result<ipnet::IpNet, _> = fake_ip.cidr.parse();
        match cidr {
            Ok(net) => {
                let (min_prefix, label) = match net {
                    ipnet::IpNet::V4(_) => (30, "/30 (4 addresses)"),
                    ipnet::IpNet::V6(_) => (120, "/120 (256 addresses)"),
                };
                if net.prefix_len() > min_prefix {
                    return Err(ConfigError::InvalidDns(format!(
                        "`dns.fake_ip.cidr` prefix length is too large for a fake IP pool; \
                         minimum is {label}",
                    )));
                }
            }
            Err(_) => {
                return Err(ConfigError::InvalidDns(format!(
                    "`dns.fake_ip.cidr` is not a valid CIDR: {}",
                    fake_ip.cidr
                )));
            }
        }
        if fake_ip.ttl_seconds == 0 {
            return Err(ConfigError::InvalidDns(
                "`dns.fake_ip.ttl_seconds` must be greater than 0".to_owned(),
            ));
        }
    }

    for (i, route) in dns.routes.iter().enumerate() {
        if route.domain.trim().is_empty() {
            return Err(ConfigError::InvalidDns(format!(
                "dns route {i}: domain must not be empty"
            )));
        }
        if route.server != "system" {
            if let Ok(idx) = route.server.parse::<usize>() {
                if idx >= num_servers {
                    return Err(ConfigError::InvalidDns(format!(
                        "dns route {i}: server index {idx} out of range (0-{})",
                        num_servers.saturating_sub(1)
                    )));
                }
            } else {
                return Err(ConfigError::InvalidDns(format!(
                    "dns route {i}: server must be \"system\" or a number (0-{})",
                    num_servers.saturating_sub(1)
                )));
            }
        }
    }

    Ok(())
}

fn validate_inbound_listen(
    seen: &mut HashSet<(String, u16)>,
    address: &str,
    port: u16,
) -> Result<(), ConfigError> {
    let key = (address.to_owned(), port);

    if !seen.insert(key.clone()) {
        return Err(ConfigError::DuplicateInboundListen {
            address: key.0,
            port: key.1,
        });
    }

    Ok(())
}
