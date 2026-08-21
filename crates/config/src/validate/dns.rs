use std::collections::{BTreeMap, HashSet};

use crate::{
    ConfigError, DnsConfig, DnsServerConfig, RuleConditionConfig, RuleSetConfig,
    RuleSetFormatConfig, RuntimeOptionsConfig,
};

use super::validate_tag;

pub(super) fn validate_dns_config(
    runtime: &RuntimeOptionsConfig,
    dns: &DnsConfig,
    rule_sets: &BTreeMap<String, RuleSetConfig>,
    rule_set_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    validate_servers(dns)?;
    validate_cache_and_answer(dns)?;

    for (index, dispatch) in dns.dispatch.iter().enumerate() {
        if !dns.servers.contains_key(&dispatch.server) {
            return Err(ConfigError::InvalidDns(format!(
                "dns dispatch {index}: references undefined server `{}`",
                dispatch.server
            )));
        }
        validate_dns_condition(&dispatch.condition, rule_sets, rule_set_tags)
            .map_err(|error| ConfigError::InvalidDns(format!("dns dispatch {index}: {error}")))?;
    }

    validate_fake_ip_tun_overlap(runtime, dns)
}

fn validate_servers(dns: &DnsConfig) -> Result<(), ConfigError> {
    if dns.servers.is_empty() {
        return Err(ConfigError::InvalidDns(
            "`dns.servers` must contain at least one named backend".to_owned(),
        ));
    }

    let mut server_tags = HashSet::new();
    for (tag, server) in &dns.servers {
        validate_tag("DNS server", tag, &mut server_tags)
            .map_err(|error| ConfigError::InvalidDns(error.to_string()))?;
        if let Some(host) = server.host() {
            if host.trim().is_empty() {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server `{tag}`: host must not be empty"
                )));
            }
            if server.port() == Some(0) {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server `{tag}`: port must be greater than 0"
                )));
            }
        }
        if let DnsServerConfig::Doh { path, .. } = server {
            if !path.starts_with('/') {
                return Err(ConfigError::InvalidDns(format!(
                    "dns server `{tag}`: DoH path must start with `/`"
                )));
            }
        }
        if matches!(
            server,
            DnsServerConfig::Udp { .. } | DnsServerConfig::Dot { .. }
        ) {
            server
                .endpoint_addresses()
                .map_err(|error| ConfigError::InvalidDns(format!("dns server `{tag}`: {error}")))?;
        }
    }

    if !dns.servers.contains_key(&dns.default_server) {
        return Err(ConfigError::InvalidDns(format!(
            "`dns.default_server` references undefined server `{}`",
            dns.default_server
        )));
    }
    Ok(())
}

fn validate_cache_and_answer(dns: &DnsConfig) -> Result<(), ConfigError> {
    if dns
        .cache
        .as_ref()
        .is_some_and(|cache| cache.max_entries == 0)
    {
        return Err(ConfigError::InvalidDns(
            "`dns.cache.max_entries` must be greater than 0".to_owned(),
        ));
    }

    let Some(fake_ip) = dns.fake_ip() else {
        return Ok(());
    };
    match fake_ip.cidr.parse::<ipnet::IpNet>() {
        Ok(ipnet::IpNet::V4(net)) if net.prefix_len() <= 30 => {}
        Ok(ipnet::IpNet::V4(_)) => {
            return Err(ConfigError::InvalidDns(
                "`dns.answer.cidr` prefix length is too large for a fake-IP pool; minimum is /30 (4 addresses)".to_owned(),
            ));
        }
        Ok(ipnet::IpNet::V6(_)) => {
            return Err(ConfigError::InvalidDns(
                "`dns.answer.cidr` currently supports IPv4 only".to_owned(),
            ));
        }
        Err(_) => {
            return Err(ConfigError::InvalidDns(format!(
                "`dns.answer.cidr` is not a valid CIDR: {}",
                fake_ip.cidr
            )));
        }
    }
    if fake_ip.ttl_seconds == 0 {
        return Err(ConfigError::InvalidDns(
            "`dns.answer.ttl_seconds` must be greater than 0".to_owned(),
        ));
    }
    Ok(())
}

fn validate_dns_condition(
    condition: &RuleConditionConfig,
    rule_sets: &BTreeMap<String, RuleSetConfig>,
    rule_set_tags: &HashSet<String>,
) -> Result<(), ConfigError> {
    match condition {
        RuleConditionConfig::Inbound { .. }
        | RuleConditionConfig::Ip { .. }
        | RuleConditionConfig::GeoIp { .. }
        | RuleConditionConfig::Sni { .. } => {
            return Err(ConfigError::InvalidRuleCondition(
                "condition requires facts unavailable before DNS resolution".to_owned(),
            ));
        }
        RuleConditionConfig::RuleSet { tag }
            if matches!(
                rule_sets.get(tag).map(|rule_set| rule_set.format),
                Some(RuleSetFormatConfig::CidrList)
            ) =>
        {
            return Err(ConfigError::InvalidRuleCondition(format!(
                "rule set `{tag}` is CIDR-only and cannot dispatch a pre-resolution DNS query"
            )));
        }
        RuleConditionConfig::And { items } | RuleConditionConfig::Or { items } => {
            for item in items {
                validate_dns_condition(item, rule_sets, rule_set_tags)?;
            }
        }
        RuleConditionConfig::Domain { .. }
        | RuleConditionConfig::DomainKeyword { .. }
        | RuleConditionConfig::DomainRegex { .. }
        | RuleConditionConfig::RuleSet { .. } => {}
    }
    condition.validate(&HashSet::new(), rule_set_tags)
}

fn validate_fake_ip_tun_overlap(
    runtime: &RuntimeOptionsConfig,
    dns: &DnsConfig,
) -> Result<(), ConfigError> {
    let (Some(tun), Some(fake_ip)) = (runtime.tun.as_ref(), dns.fake_ip()) else {
        return Ok(());
    };
    let pool = fake_ip
        .cidr
        .parse::<ipnet::IpNet>()
        .map_err(|error| ConfigError::InvalidDns(format!("invalid fake-IP CIDR: {error}")))?;
    let primary = parse_address(&tun.addr, "TUN")?;
    let secondary = if let Some(secondary) = tun.secondary_addr.as_deref() {
        Some(parse_address(secondary, "secondary TUN")?)
    } else if tun.dual_stack {
        Some(if primary.is_ipv4() {
            "fd66::1".parse().expect("static IPv6 address")
        } else {
            "10.66.0.1".parse().expect("static IPv4 address")
        })
    } else {
        None
    };

    let mut owned = vec![primary];
    if let Some(secondary) = secondary {
        owned.push(secondary);
    }
    owned.extend(owned.clone().into_iter().filter_map(next_ip));

    if let Some(address) = owned.into_iter().find(|address| pool.contains(address)) {
        return Err(ConfigError::InvalidRuntime(format!(
            "Fake-IP pool `{}` overlaps TUN-owned address `{address}`; choose a non-overlapping `dns.answer.cidr`",
            fake_ip.cidr
        )));
    }
    Ok(())
}

fn parse_address(value: &str, role: &str) -> Result<std::net::IpAddr, ConfigError> {
    value
        .split('/')
        .next()
        .unwrap_or_default()
        .parse()
        .map_err(|error| ConfigError::InvalidRuntime(format!("invalid {role} address: {error}")))
}

fn next_ip(address: std::net::IpAddr) -> Option<std::net::IpAddr> {
    match address {
        std::net::IpAddr::V4(address) => address
            .to_bits()
            .checked_add(1)
            .map(std::net::Ipv4Addr::from_bits)
            .map(std::net::IpAddr::V4),
        std::net::IpAddr::V6(address) => address
            .to_bits()
            .checked_add(1)
            .map(std::net::Ipv6Addr::from_bits)
            .map(std::net::IpAddr::V6),
    }
}
