use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::watch;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::NetworkManagement::IpHelper::{
    CancelMibChangeNotify2, FreeMibTable, GetIpForwardTable2, GetIpInterfaceEntry,
    GetIpInterfaceTable, InitializeIpInterfaceEntry, MibDeleteInstance, MibParameterNotification,
    NotifyIpInterfaceChange, NotifyRouteChange2, MIB_IPFORWARD_ROW2, MIB_IPINTERFACE_ROW,
    MIB_IPINTERFACE_TABLE, MIB_NOTIFICATION_TYPE,
};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, AF_UNSPEC, MIB_IPPROTO_ICMP, MIB_IPPROTO_LOCAL, NL_ROUTE_PROTOCOL,
    SOCKADDR_INET,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct InterfaceState {
    pub(super) connected: bool,
    pub(super) metric: u32,
    pub(super) nl_mtu: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct RouteKey {
    pub(super) interface_index: u32,
    pub(super) prefix: IpAddr,
    pub(super) prefix_length: u8,
    pub(super) next_hop: IpAddr,
}

#[derive(Debug, Default)]
pub(super) struct TopologyCache {
    interfaces: HashMap<(u64, u16), InterfaceState>,
    routes: HashMap<RouteKey, u32>,
}

impl TopologyCache {
    pub(super) fn observe_interface(
        &mut self,
        key: (u64, u16),
        notification: MIB_NOTIFICATION_TYPE,
        current: Option<InterfaceState>,
    ) -> bool {
        if notification == MibDeleteInstance {
            self.interfaces.remove(&key);
            return true;
        }
        let Some(state) = current else {
            return true;
        };
        if notification != MibParameterNotification {
            self.interfaces.insert(key, state);
            return true;
        }
        self.interfaces.insert(key, state) != Some(state)
    }

    pub(super) fn observe_route(
        &mut self,
        key: RouteKey,
        metric: u32,
        notification: MIB_NOTIFICATION_TYPE,
    ) -> bool {
        if notification == MibDeleteInstance {
            self.routes.remove(&key);
            return true;
        }
        if notification != MibParameterNotification {
            self.routes.insert(key, metric);
            return true;
        }
        self.routes.insert(key, metric) != Some(metric)
    }
}

#[derive(Debug)]
struct CallbackContext {
    generation: AtomicU64,
    sender: watch::Sender<u64>,
    topology: Mutex<TopologyCache>,
}

#[derive(Debug)]
pub(super) struct RouteChangeMonitor {
    context: Arc<CallbackContext>,
    receiver: watch::Receiver<u64>,
    route_handle: Option<usize>,
    interface_handle: Option<usize>,
    attempted: bool,
    closed: bool,
}

impl RouteChangeMonitor {
    pub(super) fn prepare() -> Self {
        let (sender, receiver) = watch::channel(0);
        Self {
            context: Arc::new(CallbackContext {
                generation: AtomicU64::new(0),
                sender,
                topology: Mutex::new(TopologyCache::default()),
            }),
            receiver,
            route_handle: None,
            interface_handle: None,
            attempted: false,
            closed: false,
        }
    }
    pub(super) fn acquire(&mut self) -> io::Result<()> {
        if self.attempted || self.closed {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "route monitor registration was already attempted",
            ));
        }
        self.attempted = true;
        seed_topology_cache(
            &mut self
                .context
                .topology
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
        );
        let callback_context = Arc::as_ptr(&self.context).cast::<c_void>();
        let mut route_handle: HANDLE = std::ptr::null_mut();
        let result = unsafe {
            NotifyRouteChange2(
                AF_UNSPEC,
                Some(route_changed),
                callback_context,
                false,
                &mut route_handle,
            )
        };
        if !route_handle.is_null() {
            self.route_handle = Some(route_handle as usize);
        }
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        let mut interface_handle: HANDLE = std::ptr::null_mut();
        let result = unsafe {
            NotifyIpInterfaceChange(
                AF_UNSPEC,
                Some(interface_changed),
                callback_context,
                false,
                &mut interface_handle,
            )
        };
        if !interface_handle.is_null() {
            self.interface_handle = Some(interface_handle as usize);
        }
        if result != 0 {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        Ok(())
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        let mut failure = None;
        for original in [&mut self.interface_handle, &mut self.route_handle] {
            if let Some(handle) = *original {
                let status = unsafe { CancelMibChangeNotify2(handle as HANDLE) };
                if status == 0 {
                    original.take();
                } else {
                    failure.get_or_insert_with(|| io::Error::from_raw_os_error(status as i32));
                }
            }
        }
        if let Some(failure) = failure {
            return Err(failure);
        }
        self.closed = true;
        Ok(())
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(super) async fn changed(&mut self) -> io::Result<()> {
        if self.closed || self.route_handle.is_none() || self.interface_handle.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "route monitor registration is incomplete or closed",
            ));
        }
        self.receiver.changed().await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Windows route notification channel closed",
            )
        })
    }

    pub(super) fn coalesce(&mut self) -> io::Result<()> {
        self.receiver.borrow_and_update();
        Ok(())
    }
}

impl Drop for RouteChangeMonitor {
    fn drop(&mut self) {
        if self.close().is_err() {
            // Compatibility callers may drop a failed close. An active native
            // callback must never point at freed context. Managed owners retain
            // this entire original and never use Drop as close evidence.
            let _ = Arc::into_raw(self.context.clone());
        }
    }
}

fn seed_topology_cache(cache: &mut TopologyCache) {
    let mut interfaces: *mut MIB_IPINTERFACE_TABLE = std::ptr::null_mut();
    if unsafe { GetIpInterfaceTable(AF_UNSPEC, &mut interfaces) } == 0 && !interfaces.is_null() {
        let rows = unsafe {
            std::slice::from_raw_parts(
                (*interfaces).Table.as_ptr(),
                (*interfaces).NumEntries as usize,
            )
        };
        for row in rows {
            let key = (unsafe { row.InterfaceLuid.Value }, row.Family);
            cache.interfaces.insert(
                key,
                InterfaceState {
                    connected: row.Connected,
                    metric: row.Metric,
                    nl_mtu: row.NlMtu,
                },
            );
        }
        unsafe { FreeMibTable(interfaces.cast()) };
    }
    let mut routes = std::ptr::null_mut();
    if unsafe { GetIpForwardTable2(AF_UNSPEC, &mut routes) } == 0 && !routes.is_null() {
        let rows = unsafe {
            std::slice::from_raw_parts((*routes).Table.as_ptr(), (*routes).NumEntries as usize)
        };
        for row in rows {
            if let Some((key, metric)) = extract_relevant_route(row) {
                cache.routes.insert(key, metric);
            }
        }
        unsafe { FreeMibTable(routes.cast()) };
    }
}

pub(super) fn is_relevant_route_prefix(
    prefix: IpAddr,
    prefix_length: u8,
    loopback: bool,
    protocol: NL_ROUTE_PROTOCOL,
) -> bool {
    if loopback || protocol == MIB_IPPROTO_LOCAL || protocol == MIB_IPPROTO_ICMP {
        return false;
    }
    if prefix_length == 0 {
        return true;
    }
    match prefix {
        IpAddr::V4(ip) => {
            !ip.is_loopback() && !ip.is_link_local() && !ip.is_multicast() && !ip.is_broadcast()
        }
        IpAddr::V6(ip) => !ip.is_loopback() && !ip.is_unicast_link_local() && !ip.is_multicast(),
    }
}

fn socket_ip(address: &SOCKADDR_INET) -> Option<IpAddr> {
    match unsafe { address.si_family } {
        AF_INET => Some(IpAddr::V4(Ipv4Addr::from(unsafe {
            address.Ipv4.sin_addr.S_un.S_addr.to_ne_bytes()
        }))),
        AF_INET6 => Some(IpAddr::V6(Ipv6Addr::from(unsafe {
            address.Ipv6.sin6_addr.u.Byte
        }))),
        _ => None,
    }
}

fn extract_relevant_route(row: &MIB_IPFORWARD_ROW2) -> Option<(RouteKey, u32)> {
    let prefix = socket_ip(&row.DestinationPrefix.Prefix)?;
    let prefix_length = row.DestinationPrefix.PrefixLength;
    if !is_relevant_route_prefix(prefix, prefix_length, row.Loopback, row.Protocol) {
        return None;
    }
    let next_hop = socket_ip(&row.NextHop)?;
    Some((
        RouteKey {
            interface_index: row.InterfaceIndex,
            prefix,
            prefix_length,
            next_hop,
        },
        row.Metric,
    ))
}

unsafe extern "system" fn route_changed(
    context: *const c_void,
    row: *const MIB_IPFORWARD_ROW2,
    notification: MIB_NOTIFICATION_TYPE,
) {
    let Some(context_ref) = (context as *const CallbackContext).as_ref() else {
        return;
    };
    let Some(row) = row.as_ref() else {
        signal(context_ref);
        return;
    };
    let Some((key, metric)) = extract_relevant_route(row) else {
        return;
    };
    let changed = context_ref
        .topology
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .observe_route(key, metric, notification);
    if changed {
        signal(context_ref);
    }
}

unsafe extern "system" fn interface_changed(
    context: *const c_void,
    row: *const MIB_IPINTERFACE_ROW,
    notification: MIB_NOTIFICATION_TYPE,
) {
    let Some(context_ref) = (context as *const CallbackContext).as_ref() else {
        return;
    };
    let Some(row) = row.as_ref() else {
        signal(context_ref);
        return;
    };
    let key = (row.InterfaceLuid.Value, row.Family);
    let current = if notification == MibDeleteInstance {
        None
    } else {
        let mut entry = MIB_IPINTERFACE_ROW::default();
        InitializeIpInterfaceEntry(&mut entry);
        entry.Family = row.Family;
        entry.InterfaceLuid = row.InterfaceLuid;
        entry.InterfaceIndex = row.InterfaceIndex;
        (GetIpInterfaceEntry(&mut entry) == 0).then_some(InterfaceState {
            connected: entry.Connected,
            metric: entry.Metric,
            nl_mtu: entry.NlMtu,
        })
    };
    let changed = context_ref
        .topology
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .observe_interface(key, notification, current);
    if changed {
        signal(context_ref);
    }
}

fn signal(context: &CallbackContext) {
    let generation = context.generation.fetch_add(1, Ordering::Relaxed) + 1;
    context.sender.send_replace(generation);
}

#[cfg(test)]
#[path = "windows/tests.rs"]
mod tests;
