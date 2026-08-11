use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use windows_sys::Win32::Foundation::ERROR_NOT_FOUND;
use windows_sys::Win32::NetworkManagement::IpHelper::{
    ConvertInterfaceAliasToLuid, ConvertInterfaceLuidToAlias, ConvertInterfaceLuidToIndex,
    CreateIpForwardEntry2, DeleteIpForwardEntry2, FreeMibTable, GetIpForwardTable2,
    InitializeIpForwardEntry, IP_ADDRESS_PREFIX, MIB_IPFORWARD_ROW2,
};
use windows_sys::Win32::NetworkManagement::Ndis::NET_LUID_LH;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, IN6_ADDR, IN6_ADDR_0, IN_ADDR, IN_ADDR_0, MIB_IPPROTO_NETMGMT, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_INET,
};

use super::{host_prefix, split_default_route_prefixes, RouteInterface, RouteJournal, RouteLease};

#[derive(Debug)]
pub struct SystemRouteGuard {
    egress: RouteInterface,
    ipv6: bool,
    gateway: IpAddr,
    tun_gateway: IpAddr,
    tun_index: u32,
    journal: RouteJournal,
}

struct WindowsInterface {
    interface_alias: String,
    interface_index: u32,
    next_hop: IpAddr,
}

impl SystemRouteGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        let lease = RouteLease::acquire(recovery_key, address.is_ipv6())?;
        recover_stale_routes(&lease, address.is_ipv6())?;
        let tun_index = interface_by_name(tun_name)?;
        let ipv6 = address.is_ipv6();
        let has_family_exclusions = excluded.iter().any(|peer| peer.is_ipv6() == ipv6);
        let physical = default_interface(ipv6, tun_index).or_else(|error| {
            if has_family_exclusions {
                Err(error)
            } else {
                default_interface(!ipv6, tun_index).map_err(|fallback| {
                    io::Error::new(
                        fallback.kind(),
                        format!(
                            "default route unavailable for the TUN address family ({error}); fallback family also unavailable ({fallback})"
                        ),
                    )
                })
            }
        })?;
        let egress = RouteInterface::new(physical.interface_alias, physical.interface_index)?;
        let journal = RouteJournal::new(
            lease,
            tun_name,
            address.is_ipv6(),
            tun_index,
            egress.clone(),
        )?;
        let mut guard = Self {
            egress,
            ipv6,
            gateway: physical.next_hop,
            tun_gateway: synthetic_tun_gateway(address)?,
            tun_index,
            journal,
        };
        for peer in excluded
            .iter()
            .copied()
            .filter(|peer| peer.is_ipv6() == address.is_ipv6())
        {
            if guard.add_exclusion(peer)? {
                guard.journal.record_exclusion(peer)?;
            }
        }
        for prefix in split_default_route_prefixes(address) {
            guard.remove(prefix)?;
            guard.add(prefix)?;
            guard.journal.record_route(prefix)?;
        }
        Ok(guard)
    }

    pub fn egress(&self) -> &RouteInterface {
        &self.egress
    }

    pub fn is_ipv6(&self) -> bool {
        self.ipv6
    }

    pub fn close(mut self) -> io::Result<()> {
        self.cleanup()
    }

    fn add(&self, prefix: &str) -> io::Result<()> {
        // Wintun is a layer-3 point-to-point adapter. Windows requires a
        // reachable next hop for newly opened TCP sockets; an on-link
        // 0.0.0.0/:: route can be listed as Alive yet fail source/next-hop
        // selection with WSAEHOSTUNREACH. Use an on-link synthetic peer as
        // the gateway, as Windows VPN clients conventionally do.
        create_route(self.tun_index, prefix, self.tun_gateway)
    }

    fn remove(&self, prefix: &str) -> io::Result<()> {
        remove_route(self.tun_index, prefix)
    }

    fn add_exclusion(&self, peer: IpAddr) -> io::Result<bool> {
        let prefix = host_prefix(peer);
        if matching_routes(self.egress.index(), &prefix, Some(self.gateway))?.is_empty() {
            create_route(self.egress.index(), &prefix, self.gateway)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let tun_index = self.tun_index;
        let egress_index = self.egress.index();
        self.journal.cleanup(
            |prefix| remove_route(tun_index, prefix),
            |peer| remove_exclusion(egress_index, peer),
        )
    }
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let tun_index = journal.tun_index;
    let egress_index = journal.egress.index();
    journal.cleanup(
        |prefix| remove_route(tun_index, prefix),
        |peer| remove_exclusion(egress_index, peer),
    )
}

fn remove_route(tun_index: u32, prefix: &str) -> io::Result<()> {
    delete_routes(matching_routes(tun_index, prefix, None)?)
}

fn remove_exclusion(egress_index: u32, peer: IpAddr) -> io::Result<()> {
    delete_routes(matching_routes(egress_index, &host_prefix(peer), None)?)
}

impl Drop for SystemRouteGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

fn interface_by_name(name: &str) -> io::Result<u32> {
    let name = name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut luid = NET_LUID_LH::default();
    win32_result("resolve TUN interface alias", unsafe {
        ConvertInterfaceAliasToLuid(name.as_ptr(), &mut luid)
    })?;
    let mut index = 0;
    win32_result("resolve TUN interface index", unsafe {
        ConvertInterfaceLuidToIndex(&luid, &mut index)
    })?;
    Ok(index)
}

fn default_interface(ipv6: bool, tun_index: u32) -> io::Result<WindowsInterface> {
    let family = if ipv6 { AF_INET6 } else { AF_INET };
    let routes = route_table(family)?;
    let defaults = routes
        .iter()
        .filter(|route| route.DestinationPrefix.PrefixLength == 0)
        .map(|route| (route.InterfaceIndex, route.Metric))
        .collect::<Vec<_>>();
    let samples = routes
        .iter()
        .take(16)
        .map(|route| {
            (
                route.InterfaceIndex,
                route.DestinationPrefix.PrefixLength,
                socket_ip(&route.DestinationPrefix.Prefix).ok(),
                route.Metric,
            )
        })
        .collect::<Vec<_>>();
    let row_count = routes.len();
    let route = routes
        .into_iter()
        .filter(|route| route.InterfaceIndex != tun_index)
        .filter(|route| route.DestinationPrefix.PrefixLength == 0)
        .min_by_key(|route| route.Metric)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "default route not found for family {family}; TUN index {tun_index}; zero-prefix candidates {defaults:?}; rows={}; samples={samples:?}",
                    row_count
                ),
            )
        })?;
    Ok(WindowsInterface {
        interface_alias: interface_alias(&route.InterfaceLuid)?,
        interface_index: route.InterfaceIndex,
        next_hop: socket_ip(&route.NextHop)?,
    })
}

fn interface_alias(luid: &NET_LUID_LH) -> io::Result<String> {
    let mut alias = [0_u16; 257];
    win32_result("resolve physical interface alias", unsafe {
        ConvertInterfaceLuidToAlias(luid, alias.as_mut_ptr(), alias.len())
    })?;
    let length = alias
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(alias.len());
    String::from_utf16(&alias[..length]).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("decode physical interface alias: {error}"),
        )
    })
}

fn create_route(index: u32, prefix: &str, next_hop: IpAddr) -> io::Result<()> {
    let row = route_row(index, prefix, next_hop)?;
    win32_result(&format!("create Windows route `{prefix}`"), unsafe {
        CreateIpForwardEntry2(&row)
    })
}

fn delete_routes(routes: Vec<MIB_IPFORWARD_ROW2>) -> io::Result<()> {
    for route in routes {
        let result = unsafe { DeleteIpForwardEntry2(&route) };
        if result != 0 && result != ERROR_NOT_FOUND {
            return win32_result("delete Windows route", result);
        }
    }
    Ok(())
}

fn matching_routes(
    index: u32,
    prefix: &str,
    next_hop: Option<IpAddr>,
) -> io::Result<Vec<MIB_IPFORWARD_ROW2>> {
    let (network, length) = parse_prefix(prefix)?;
    let family = if network.is_ipv6() { AF_INET6 } else { AF_INET };
    Ok(route_table(family)?
        .into_iter()
        .filter(|row| {
            row.InterfaceIndex == index
                && row.DestinationPrefix.PrefixLength == length
                && socket_ip(&row.DestinationPrefix.Prefix).ok() == Some(network)
                && next_hop.is_none_or(|next_hop| socket_ip(&row.NextHop).ok() == Some(next_hop))
        })
        .collect())
}

fn route_table(
    family: windows_sys::Win32::Networking::WinSock::ADDRESS_FAMILY,
) -> io::Result<Vec<MIB_IPFORWARD_ROW2>> {
    let mut table = std::ptr::null_mut();
    win32_result("query Windows route table", unsafe {
        GetIpForwardTable2(family, &mut table)
    })?;
    if table.is_null() {
        return Err(io::Error::other("Windows route table pointer is null"));
    }
    let rows = unsafe {
        std::slice::from_raw_parts((*table).Table.as_ptr(), (*table).NumEntries as usize)
    };
    let result = rows.to_vec();
    unsafe { FreeMibTable(table.cast()) };
    Ok(result)
}

fn route_row(index: u32, prefix: &str, next_hop: IpAddr) -> io::Result<MIB_IPFORWARD_ROW2> {
    let (network, length) = parse_prefix(prefix)?;
    if network.is_ipv4() != next_hop.is_ipv4() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows route prefix and next hop families differ",
        ));
    }
    let mut row = MIB_IPFORWARD_ROW2::default();
    unsafe { InitializeIpForwardEntry(&mut row) };
    row.InterfaceIndex = index;
    row.DestinationPrefix = IP_ADDRESS_PREFIX {
        Prefix: socket_address(network),
        PrefixLength: length,
    };
    row.NextHop = socket_address(next_hop);
    row.Metric = 0;
    row.Protocol = MIB_IPPROTO_NETMGMT;
    Ok(row)
}

fn parse_prefix(prefix: &str) -> io::Result<(IpAddr, u8)> {
    let (address, length) = prefix.split_once('/').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid route `{prefix}`"),
        )
    })?;
    let address = address.parse::<IpAddr>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid route address `{address}`: {error}"),
        )
    })?;
    let length = length.parse::<u8>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid route prefix length `{length}`: {error}"),
        )
    })?;
    if length > if address.is_ipv4() { 32 } else { 128 } {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid route prefix length `{length}`"),
        ));
    }
    Ok((address, length))
}

fn socket_address(address: IpAddr) -> SOCKADDR_INET {
    match address {
        IpAddr::V4(address) => SOCKADDR_INET {
            Ipv4: SOCKADDR_IN {
                sin_family: AF_INET,
                sin_port: 0,
                sin_addr: IN_ADDR {
                    S_un: IN_ADDR_0 {
                        S_addr: u32::from_ne_bytes(address.octets()),
                    },
                },
                sin_zero: [0; 8],
            },
        },
        IpAddr::V6(address) => SOCKADDR_INET {
            Ipv6: SOCKADDR_IN6 {
                sin6_family: AF_INET6,
                sin6_port: 0,
                sin6_flowinfo: 0,
                sin6_addr: IN6_ADDR {
                    u: IN6_ADDR_0 {
                        Byte: address.octets(),
                    },
                },
                Anonymous: Default::default(),
            },
        },
    }
}

fn socket_ip(address: &SOCKADDR_INET) -> io::Result<IpAddr> {
    match unsafe { address.si_family } {
        AF_INET => Ok(IpAddr::V4(Ipv4Addr::from(unsafe {
            address.Ipv4.sin_addr.S_un.S_addr.to_ne_bytes()
        }))),
        AF_INET6 => Ok(IpAddr::V6(Ipv6Addr::from(unsafe {
            address.Ipv6.sin6_addr.u.Byte
        }))),
        family => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported Windows route address family `{family}`"),
        )),
    }
}

fn synthetic_tun_gateway(address: IpAddr) -> io::Result<IpAddr> {
    match address {
        IpAddr::V4(address) if !address.is_unspecified() => {
            let bits = address.to_bits();
            let peer = bits.checked_add(1).unwrap_or(bits - 1);
            Ok(IpAddr::V4(Ipv4Addr::from_bits(peer)))
        }
        IpAddr::V6(address) if !address.is_unspecified() => {
            let bits = address.to_bits();
            let peer = bits.checked_add(1).unwrap_or(bits - 1);
            Ok(IpAddr::V6(Ipv6Addr::from_bits(peer)))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows automatic TUN routes require a configured address for this family",
        )),
    }
}

fn win32_result(context: &str, result: u32) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{context}: {}",
            io::Error::from_raw_os_error(result as i32)
        )))
    }
}
