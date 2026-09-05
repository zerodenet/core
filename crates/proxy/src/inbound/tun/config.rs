use std::io;
use std::net::IpAddr;

use zero_engine::EngineError;

pub(super) const DEFAULT_TUN_IPV4_ADDR: &str = "10.66.0.1/24";
pub(super) const DEFAULT_TUN_IPV6_ADDR: &str = "fd66::1/64";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TunInterfaceAddress {
    pub address: IpAddr,
    pub netmask: IpAddr,
    pub cidr: String,
}

pub(super) fn parse_interface_addresses(
    addr: &str,
    mask: &str,
    secondary_addr: Option<&str>,
    dual_stack: bool,
) -> Result<Vec<TunInterfaceAddress>, EngineError> {
    let (address, netmask) = parse_address_and_mask(addr, mask)?;
    let mut addresses = vec![interface_address(address, netmask)?];

    if !dual_stack {
        if secondary_addr.is_some() {
            return Err(invalid_input(
                "TUN secondary address requires dual-stack mode",
            ));
        }
        return Ok(addresses);
    }

    let secondary_addr = secondary_addr.unwrap_or(if address.is_ipv4() {
        DEFAULT_TUN_IPV6_ADDR
    } else {
        DEFAULT_TUN_IPV4_ADDR
    });
    if !secondary_addr.contains('/') {
        return Err(invalid_input(
            "TUN secondary address must use CIDR notation",
        ));
    }
    let (secondary_address, secondary_mask) = parse_address_and_mask(secondary_addr, "")?;
    if secondary_address.is_ipv4() == address.is_ipv4() {
        return Err(invalid_input(
            "TUN primary and secondary addresses must use different address families",
        ));
    }
    addresses.push(interface_address(secondary_address, secondary_mask)?);
    Ok(addresses)
}

fn interface_address(address: IpAddr, netmask: IpAddr) -> Result<TunInterfaceAddress, EngineError> {
    let prefix = zero_tun::mask_to_prefix(netmask).map_err(EngineError::Io)?;
    Ok(TunInterfaceAddress {
        address,
        netmask,
        cidr: format!("{address}/{prefix}"),
    })
}

pub(super) fn parse_address_and_mask(
    addr: &str,
    mask: &str,
) -> Result<(IpAddr, IpAddr), EngineError> {
    if let Some((address, prefix)) = addr.split_once('/') {
        let address = parse_ip(address, "TUN address")?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|error| invalid_input(format!("invalid TUN prefix `{prefix}`: {error}")))?;
        let max_prefix = if address.is_ipv4() { 32 } else { 128 };
        if prefix > max_prefix {
            return Err(invalid_input(format!(
                "invalid TUN prefix `{prefix}` for {address}"
            )));
        }
        let derived = zero_tun::prefix_to_mask(prefix, address.is_ipv6());
        if !mask.trim().is_empty() {
            let configured = parse_ip(mask, "TUN mask")?;
            // An unspecified address is the historical sentinel used by
            // callers that already supplied an authoritative CIDR prefix.
            if !configured.is_unspecified() {
                if configured.is_ipv4() != address.is_ipv4() {
                    return Err(invalid_input("TUN address and mask families differ"));
                }
                if configured != derived {
                    return Err(invalid_input(format!(
                        "TUN address prefix /{prefix} and mask {configured} describe different networks"
                    )));
                }
            }
        }
        return Ok((address, derived));
    }

    let address = parse_ip(addr, "TUN address")?;
    let netmask = parse_ip(mask, "TUN mask")?;
    if address.is_ipv4() != netmask.is_ipv4() {
        return Err(invalid_input("TUN address and mask families differ"));
    }
    Ok((address, netmask))
}

#[cfg(test)]
pub(super) fn configured_dns_endpoint_addresses(
    config: &zero_config::RuntimeConfig,
) -> io::Result<Vec<IpAddr>> {
    configured_dns_endpoint_addresses_with(config, zero_platform_tokio::system_dns_servers)
}

pub(super) fn configured_dns_endpoint_addresses_with(
    config: &zero_config::RuntimeConfig,
    discover_system_dns: impl FnOnce() -> io::Result<Vec<IpAddr>>,
) -> io::Result<Vec<IpAddr>> {
    let Some(dns) = config.runtime.dns.as_ref() else {
        let mut addresses = discover_system_dns()?;
        addresses.sort_unstable();
        addresses.dedup();
        return Ok(addresses);
    };
    let mut addresses = dns
        .tun_route_exclusion_addresses()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    if dns.uses_system_dns() {
        addresses.extend(discover_system_dns()?);
    }
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

/// DNS interception and protecting host DNS egress are independent concerns.
pub(super) fn prepare_dns_routes(
    config: &zero_config::RuntimeConfig,
    hijack: bool,
    auto_route: bool,
    discover_system_dns: impl FnOnce() -> io::Result<Vec<IpAddr>>,
) -> io::Result<(bool, Vec<IpAddr>)> {
    if hijack && config.runtime.dns.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TUN DNS hijack requires a configured DNS server",
        ));
    }
    if !auto_route {
        return Ok((hijack, Vec::new()));
    }
    // Without interception, applications still use the OS resolver even if
    // the kernel itself uses exclusively explicit DNS backends.
    let extra_system = !hijack
        && config
            .runtime
            .dns
            .as_ref()
            .is_some_and(|dns| !dns.uses_system_dns());
    let mut endpoints = if extra_system {
        let mut endpoints = configured_dns_endpoint_addresses_with(config, || Ok(Vec::new()))?;
        endpoints.extend(discover_system_dns()?);
        endpoints
    } else {
        configured_dns_endpoint_addresses_with(config, discover_system_dns)?
    };
    endpoints.sort_unstable();
    endpoints.dedup();
    Ok((hijack, endpoints))
}

fn parse_ip(value: &str, field: &str) -> Result<IpAddr, EngineError> {
    value
        .parse()
        .map_err(|error| invalid_input(format!("invalid {field} `{value}`: {error}")))
}

fn invalid_input(message: impl Into<String>) -> EngineError {
    EngineError::Io(io::Error::new(io::ErrorKind::InvalidInput, message.into()))
}
