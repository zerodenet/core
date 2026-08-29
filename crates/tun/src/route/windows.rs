use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipnet::IpNet;
use windows_sys::Win32::Foundation::ERROR_NOT_FOUND;
use windows_sys::Win32::NetworkManagement::IpHelper::{
    ConvertInterfaceAliasToLuid, ConvertInterfaceLuidToAlias, ConvertInterfaceLuidToIndex,
    CreateIpForwardEntry2, DeleteIpForwardEntry2, FreeMibTable, GetIpForwardTable2,
    GetIpInterfaceEntry, GetUnicastIpAddressTable, InitializeIpForwardEntry,
    InitializeIpInterfaceEntry, IP_ADDRESS_PREFIX, MIB_IPFORWARD_ROW2, MIB_IPINTERFACE_ROW,
};
use windows_sys::Win32::NetworkManagement::Ndis::NET_LUID_LH;
use windows_sys::Win32::Networking::WinSock::{
    IpDadStatePreferred, AF_INET, AF_INET6, IN6_ADDR, IN6_ADDR_0, IN_ADDR, IN_ADDR_0,
    MIB_IPPROTO_NETMGMT, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

use super::reconcile::{reconcile_route_state, with_rollback_error, RouteReconcileState};
use super::{
    family_exclusions, host_prefix, EgressUnavailableReason, FamilyEgressState, RouteInterface,
    RouteJournal, RouteLease,
};

#[derive(Debug)]
pub struct SystemRouteGuard {
    /// Carrier used by route installation and exclusion maintenance.
    egress: RouteInterface,
    /// Native socket reachability for the TUN address family.
    family_egress: FamilyEgressState,
    ipv6: bool,
    gateway: IpAddr,
    tun_gateway: IpAddr,
    tun_index: u32,
    excluded: Vec<IpAddr>,
    journal: RouteJournal,
}

struct WindowsInterface {
    interface_alias: String,
    interface_index: u32,
    next_hop: IpAddr,
}

struct WindowsEgressSelection {
    carrier: RouteInterface,
    gateway: IpAddr,
    family: FamilyEgressState,
}

impl SystemRouteGuard {
    pub fn install(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        netmask: IpAddr,
        captured: &[IpNet],
        excluded: &[IpAddr],
    ) -> io::Result<Self> {
        Self::install_with_egress(
            tun_name,
            recovery_key,
            address,
            netmask,
            captured,
            excluded,
            |_| Ok(()),
        )
    }

    pub fn install_with_egress(
        tun_name: &str,
        recovery_key: &str,
        address: IpAddr,
        _netmask: IpAddr,
        captured: &[IpNet],
        excluded: &[IpAddr],
        publish_egress: impl FnOnce(&FamilyEgressState) -> io::Result<()>,
    ) -> io::Result<Self> {
        let lease = RouteLease::acquire(recovery_key, address.is_ipv6())?;
        recover_stale_routes(&lease, address.is_ipv6())?;
        let tun_index = interface_by_name(tun_name)?;
        let ipv6 = address.is_ipv6();
        let has_family_exclusions = excluded.iter().any(|peer| peer.is_ipv6() == ipv6);
        let selected = select_physical_egress(ipv6, tun_index, !has_family_exclusions)?;
        let egress = selected.carrier;
        let gateway = selected.gateway;
        let desired_exclusions = family_exclusions(excluded, ipv6);
        let journal = RouteJournal::new(
            lease,
            tun_name,
            address.is_ipv6(),
            tun_index,
            egress.clone(),
            Some(gateway.to_string()),
        )?;
        let mut guard = Self {
            egress,
            family_egress: selected.family,
            ipv6,
            gateway,
            tun_gateway: synthetic_tun_gateway(address)?,
            tun_index,
            excluded: desired_exclusions.clone(),
            journal,
        };
        publish_egress(&guard.family_egress)?;
        for peer in desired_exclusions {
            guard.install_exclusion(peer)?;
        }
        for prefix in captured {
            let prefix = prefix.to_string();
            guard.remove(&prefix)?;
            guard.add(&prefix)?;
            guard.journal.record_route(&prefix)?;
        }
        Ok(guard)
    }

    pub fn egress(&self) -> &RouteInterface {
        &self.egress
    }

    pub fn family_egress(&self) -> &FamilyEgressState {
        &self.family_egress
    }

    pub fn is_ipv6(&self) -> bool {
        self.ipv6
    }

    /// Re-resolve the preferred physical interface and reconcile explicit
    /// bypass routes without replacing the TUN device or split default routes.
    pub fn reconcile(&mut self, excluded: &[IpAddr]) -> io::Result<bool> {
        let desired_exclusions = family_exclusions(excluded, self.ipv6);
        let has_family_exclusions = !desired_exclusions.is_empty();
        let selected = select_physical_egress(self.ipv6, self.tun_index, !has_family_exclusions)?;
        let family_changed = self.family_egress != selected.family;
        let changed =
            reconcile_route_state(self, selected.carrier, selected.gateway, desired_exclusions)?;
        self.family_egress = selected.family;
        Ok(changed || family_changed)
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

    fn install_exclusion(&mut self, peer: IpAddr) -> io::Result<()> {
        let prefix = host_prefix(peer);
        if !matching_routes(self.egress.index(), &prefix, Some(self.gateway))?.is_empty() {
            return Ok(());
        }
        self.journal.record_exclusion(peer)?;
        if let Err(error) = create_route(self.egress.index(), &prefix, self.gateway) {
            let rollback = self.journal.forget_exclusion(peer);
            return Err(with_rollback_error(error, rollback));
        }
        Ok(())
    }

    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()> {
        let added = desired
            .iter()
            .copied()
            .filter(|peer| !self.excluded.contains(peer))
            .collect::<Vec<_>>();
        for peer in added {
            self.install_exclusion(peer)?;
        }
        let stale = self
            .excluded
            .clone()
            .into_iter()
            .filter(|peer| !desired.contains(peer) && self.journal.excluded.contains(peer))
            .collect::<Vec<_>>();
        for peer in stale {
            remove_exclusion(self.egress.index(), Some(self.gateway), peer)?;
            self.journal.forget_exclusion(peer)?;
        }
        Ok(())
    }

    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()> {
        for peer in excluded.iter().copied() {
            self.install_exclusion(peer)?;
        }
        Ok(())
    }

    fn remove_owned_exclusions(&mut self) -> io::Result<()> {
        for peer in self.journal.excluded.clone().into_iter().rev() {
            remove_exclusion(self.egress.index(), Some(self.gateway), peer)?;
            self.journal.forget_exclusion(peer)?;
        }
        Ok(())
    }

    fn cleanup(&mut self) -> io::Result<()> {
        let tun_index = self.tun_index;
        let egress_index = self.egress.index();
        self.journal.cleanup(
            |prefix| remove_route(tun_index, prefix),
            |peer| remove_exclusion(egress_index, Some(self.gateway), peer),
        )
    }
}

impl RouteReconcileState for SystemRouteGuard {
    type Gateway = IpAddr;

    fn current_egress(&self) -> &RouteInterface {
        &self.egress
    }

    fn current_gateway(&self) -> &Self::Gateway {
        &self.gateway
    }

    fn current_exclusions(&self) -> &[IpAddr] {
        &self.excluded
    }

    fn owned_exclusions(&self) -> Vec<IpAddr> {
        self.journal.excluded.clone()
    }

    fn reconcile_exclusions(&mut self, desired: &[IpAddr]) -> io::Result<()> {
        SystemRouteGuard::reconcile_exclusions(self, desired)
    }

    fn remove_owned_exclusions(&mut self) -> io::Result<()> {
        SystemRouteGuard::remove_owned_exclusions(self)
    }

    fn replace_egress(&mut self, egress: RouteInterface, gateway: Self::Gateway) -> io::Result<()> {
        self.journal
            .replace_egress(egress.clone(), Some(gateway.to_string()))?;
        self.egress = egress;
        self.gateway = gateway;
        Ok(())
    }

    fn install_exclusions(&mut self, excluded: &[IpAddr]) -> io::Result<()> {
        SystemRouteGuard::install_exclusions(self, excluded)
    }

    fn set_current_exclusions(&mut self, excluded: Vec<IpAddr>) {
        self.excluded = excluded;
    }
}

fn recover_stale_routes(lease: &RouteLease, ipv6: bool) -> io::Result<()> {
    let Some(mut journal) = RouteJournal::load(lease, ipv6)? else {
        return Ok(());
    };
    let tun_index = journal.tun_index;
    let egress_index = journal.egress.index();
    let gateway = journal
        .gateway
        .as_deref()
        .map(str::parse::<IpAddr>)
        .transpose()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse TUN route recovery gateway: {error}"),
            )
        })?;
    journal.cleanup(
        |prefix| remove_route(tun_index, prefix),
        |peer| remove_exclusion(egress_index, gateway, peer),
    )
}

fn remove_route(tun_index: u32, prefix: &str) -> io::Result<()> {
    delete_routes(matching_routes(tun_index, prefix, None)?)
}

fn remove_exclusion(egress_index: u32, gateway: Option<IpAddr>, peer: IpAddr) -> io::Result<()> {
    delete_routes(matching_routes(egress_index, &host_prefix(peer), gateway)?)
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

fn select_physical_egress(
    ipv6: bool,
    tun_index: u32,
    allow_cross_family_carrier: bool,
) -> io::Result<WindowsEgressSelection> {
    match default_interface(ipv6, tun_index) {
        Ok(physical) => {
            let carrier =
                RouteInterface::new(physical.interface_alias.clone(), physical.interface_index)?;
            let family = match native_family_unavailable_reason(ipv6, physical.interface_index) {
                Ok(None) => FamilyEgressState::Available(carrier.clone()),
                Ok(Some(reason)) => {
                    tracing::warn!(
                        family = if ipv6 { "ipv6" } else { "ipv4" },
                        route_install_carrier = %physical.interface_alias,
                        route_install_carrier_index = physical.interface_index,
                        native_egress_state = "unavailable",
                        native_egress_reason = reason.as_str(),
                        "TUN route carrier does not provide usable native family egress"
                    );
                    FamilyEgressState::Unavailable(reason)
                }
                Err(error) => {
                    tracing::warn!(
                        family = if ipv6 { "ipv6" } else { "ipv4" },
                        route_install_carrier = %physical.interface_alias,
                        route_install_carrier_index = physical.interface_index,
                        native_egress_state = "unavailable",
                        native_egress_reason = EgressUnavailableReason::RouteLookupFailed.as_str(),
                        error = %error,
                        "failed to verify native family egress for the TUN route carrier"
                    );
                    FamilyEgressState::Unavailable(EgressUnavailableReason::RouteLookupFailed)
                }
            };
            Ok(WindowsEgressSelection {
                carrier,
                gateway: physical.next_hop,
                family,
            })
        }
        Err(native_error) if allow_cross_family_carrier => {
            let physical = default_interface(!ipv6, tun_index).map_err(|fallback| {
                io::Error::new(
                    fallback.kind(),
                    format!(
                        "default route unavailable for the TUN address family ({native_error}); fallback family also unavailable ({fallback})"
                    ),
                )
            })?;
            let carrier =
                RouteInterface::new(physical.interface_alias.clone(), physical.interface_index)?;
            tracing::info!(
                family = if ipv6 { "ipv6" } else { "ipv4" },
                route_install_carrier = %physical.interface_alias,
                route_install_carrier_index = physical.interface_index,
                route_install_carrier_family = if ipv6 { "ipv4" } else { "ipv6" },
                native_egress_state = "unavailable",
                native_egress_reason = EgressUnavailableReason::NoDefaultRoute.as_str(),
                native_route_error = %native_error,
                "selected a cross-family TUN route carrier without publishing native egress"
            );
            Ok(WindowsEgressSelection {
                carrier,
                gateway: physical.next_hop,
                family: FamilyEgressState::Unavailable(EgressUnavailableReason::NoDefaultRoute),
            })
        }
        Err(error) => Err(error),
    }
}

fn native_family_unavailable_reason(
    ipv6: bool,
    interface_index: u32,
) -> io::Result<Option<EgressUnavailableReason>> {
    let family = if ipv6 { AF_INET6 } else { AF_INET };
    let mut interface = MIB_IPINTERFACE_ROW::default();
    unsafe {
        InitializeIpInterfaceEntry(&mut interface);
    }
    interface.Family = family;
    interface.InterfaceIndex = interface_index;
    win32_result("query physical IP interface", unsafe {
        GetIpInterfaceEntry(&mut interface)
    })?;
    if !interface.Connected {
        return Ok(Some(EgressUnavailableReason::InterfaceDown));
    }
    if !interface_has_usable_address(family, interface_index)? {
        return Ok(Some(EgressUnavailableReason::NoUsableAddress));
    }
    Ok(None)
}

fn interface_has_usable_address(family: u16, interface_index: u32) -> io::Result<bool> {
    let mut table = std::ptr::null_mut();
    win32_result("query physical interface addresses", unsafe {
        GetUnicastIpAddressTable(family, &mut table)
    })?;
    if table.is_null() {
        return Err(io::Error::other(
            "Windows unicast address table pointer is null",
        ));
    }
    let rows = unsafe {
        std::slice::from_raw_parts((*table).Table.as_ptr(), (*table).NumEntries as usize)
    };
    let found = rows.iter().any(|row| {
        row.InterfaceIndex == interface_index
            && row.DadState == IpDadStatePreferred
            && socket_ip(&row.Address).is_ok_and(usable_unicast_address)
    });
    unsafe { FreeMibTable(table.cast()) };
    Ok(found)
}

fn usable_unicast_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_link_local()
                && !address.is_multicast()
                && !address.is_broadcast()
        }
        IpAddr::V6(address) => {
            !address.is_unspecified()
                && !address.is_loopback()
                && !address.is_unicast_link_local()
                && !address.is_multicast()
        }
    }
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
        .min_by_key(|route| effective_metric(route, family))
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

fn effective_metric(route: &MIB_IPFORWARD_ROW2, family: u16) -> u64 {
    let mut interface = MIB_IPINTERFACE_ROW::default();
    unsafe {
        InitializeIpInterfaceEntry(&mut interface);
    }
    interface.Family = family;
    interface.InterfaceLuid = route.InterfaceLuid;
    interface.InterfaceIndex = route.InterfaceIndex;
    let interface_metric = if unsafe { GetIpInterfaceEntry(&mut interface) } == 0 {
        interface.Metric
    } else {
        0
    };
    u64::from(route.Metric) + u64::from(interface_metric)
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
