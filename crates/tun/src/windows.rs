//! Windows TUN device via Wintun.
//!
//! Uses WireGuard's Wintun driver for virtual network interfaces.
//! I/O is bridged from synchronous Wintun sessions to async tokio
//! via mpsc channels.
//!
//! ## wintun.dll resolution order
//!
//! 1. Binary-adjacent `wintun.dll` (same directory as the executable)
//! 2. `PATH` / system library search
//!
//! To bundle: place `wintun.dll` (from <https://wintun.net>) next to
//! the `zero` binary.  Release builds should ship it alongside.

use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_NOT_FOUND, ERROR_OBJECT_ALREADY_EXISTS};
use windows_sys::Win32::NetworkManagement::IpHelper::{
    ConvertInterfaceAliasToLuid, CreateUnicastIpAddressEntry, DeleteUnicastIpAddressEntry,
    FreeMibTable, GetIpInterfaceEntry, GetUnicastIpAddressTable, InitializeIpInterfaceEntry,
    InitializeUnicastIpAddressEntry, SetIpInterfaceEntry, MIB_IPINTERFACE_ROW,
    MIB_UNICASTIPADDRESS_ROW,
};
use windows_sys::Win32::NetworkManagement::Ndis::NET_LUID_LH;
use windows_sys::Win32::Networking::WinSock::{
    IpPrefixOriginManual, AF_INET, AF_INET6, AF_UNSPEC, IN6_ADDR, IN6_ADDR_0, IN_ADDR, IN_ADDR_0,
    SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, TokenElevation, TOKEN_ELEVATION, TOKEN_QUERY,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use crate::TunDevice;

/// A Windows TUN device backed by Wintun.
///
/// Reads from a receiver filled by a background Wintun reader thread;
/// writes go to a sender consumed by a background Wintun writer thread.
pub struct WindowsTun {
    name: String,
    rx: mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<Vec<u8>>,
    _session: Arc<wintun::Session>,
    _adapter: Arc<wintun::Adapter>,
}

impl WindowsTun {
    /// Create a new Wintun TUN adapter.  `name` is the adapter name
    /// (e.g. `"ZeroTun"`).  The Wintun DLL must be available on the
    /// system (bundled with the binary or in `PATH`).
    pub fn create(name: Option<&str>) -> io::Result<Self> {
        require_elevated_process()?;
        let wintun = load_wintun()?;

        let adapter_name = name.unwrap_or("ZeroTun");

        let adapter = wintun::Adapter::open(&wintun, adapter_name)
            // A failed Windows device installation can leave its requested
            // GUID reserved even though no named adapter can be opened. Let
            // Wintun allocate a fresh GUID so a later start can recover.
            .or_else(|_| wintun::Adapter::create(&wintun, adapter_name, "ZeroTun", None))
            .map_err(|e| io::Error::other(format!("wintun open/create adapter: {e}")))?;

        let session = Arc::new(
            adapter
                .start_session(wintun::MAX_RING_CAPACITY)
                .map_err(|e| io::Error::other(format!("wintun start session: {e}")))?,
        );

        // Bridge Wintun (sync) ↔ tokio (async) via channels.
        let (read_tx, read_rx) = mpsc::channel::<Vec<u8>>(256);
        let (write_tx, mut write_rx) = mpsc::channel::<Vec<u8>>(256);

        // Reader thread.
        let reader_session = session.clone();
        std::thread::spawn(move || {
            while let Ok(pkt) = reader_session.receive_blocking() {
                let data = pkt.bytes().to_vec();
                if read_tx.blocking_send(data).is_err() {
                    break; // channel closed
                }
            }
        });

        // Writer thread.
        let writer_session = session.clone();
        std::thread::spawn(move || {
            while let Some(data) = write_rx.blocking_recv() {
                let len = data.len().min(u16::MAX as usize) as u16;
                match writer_session.allocate_send_packet(len) {
                    Ok(mut pkt) => {
                        pkt.bytes_mut()[..len as usize].copy_from_slice(&data[..len as usize]);
                        writer_session.send_packet(pkt);
                    }
                    Err(error) => {
                        tracing::warn!(%error, "Wintun packet allocation failed");
                        break;
                    }
                }
            }
            let _ = writer_session.shutdown();
        });

        Ok(Self {
            name: adapter_name.to_owned(),
            rx: read_rx,
            tx: write_tx,
            _session: session,
            _adapter: adapter,
        })
    }
}

const WINDOWS_TUN_ELEVATION_MESSAGE: &str =
    "Windows TUN requires an elevated Administrator process";

fn require_elevated_process() -> io::Result<()> {
    validate_elevation(process_is_elevated()?)
}

fn validate_elevation(is_elevated: bool) -> io::Result<()> {
    if is_elevated {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            WINDOWS_TUN_ELEVATION_MESSAGE,
        ))
    }
}

fn process_is_elevated() -> io::Result<bool> {
    let mut token = std::ptr::null_mut();
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("inspect Windows process elevation: {error}"),
        ));
    }

    let mut elevation = TOKEN_ELEVATION::default();
    let mut returned = 0;
    let result = unsafe {
        GetTokenInformation(
            token,
            TokenElevation,
            std::ptr::addr_of_mut!(elevation).cast(),
            std::mem::size_of::<TOKEN_ELEVATION>() as u32,
            &mut returned,
        )
    };
    let error = (result == 0).then(io::Error::last_os_error);
    unsafe {
        CloseHandle(token);
    }
    if let Some(error) = error {
        return Err(io::Error::new(
            error.kind(),
            format!("inspect Windows process elevation: {error}"),
        ));
    }
    Ok(elevation.TokenIsElevated != 0)
}

/// Load wintun.dll, trying binary-adjacent first, then system path.
fn load_wintun() -> io::Result<wintun::Wintun> {
    // 1. Try binary-adjacent `wintun.dll`.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let adjacent = dir.join("wintun.dll");
            if adjacent.exists() {
                return unsafe { wintun::load_from_path(&adjacent) }.map_err(|e| {
                    io::Error::other(format!(
                        "wintun load from {} failed: {e}",
                        adjacent.display()
                    ))
                });
            }
        }
    }

    // 2. Fall back to system PATH.
    unsafe { wintun::load() }.map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "wintun.dll not found\n\
             \n\
             Download from https://wintun.net and place wintun.dll:\n\
               • next to zero.exe (binary-adjacent), or\n\
               • anywhere in %PATH%\n\
             \n\
             On Linux/macOS: TUN works without extra drivers.",
        )
    })
}

impl AsyncRead for WindowsTun {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                let n = data.len().min(buf.remaining());
                buf.put_slice(&data[..n]);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for WindowsTun {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.tx.try_send(buf.to_vec()) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(mpsc::error::TrySendError::Full(_)) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "tun closed")))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl TunDevice for WindowsTun {
    fn configure(&self, addr: IpAddr, mask: IpAddr, mtu: u16) -> io::Result<()> {
        configure_adapter(&self.name, &[(addr, mask)], mtu)
    }

    fn configure_addresses(&self, addresses: &[(IpAddr, IpAddr)], mtu: u16) -> io::Result<()> {
        configure_adapter(&self.name, addresses, mtu)
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn into_channels(mut self) -> io::Result<(mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>)> {
        let (closed_tx, closed_rx) = mpsc::channel(1);
        drop(closed_tx);
        let receiver = std::mem::replace(&mut self.rx, closed_rx);
        // Dropping `self` here releases the original sender. The returned
        // sender is then the sole owner; when its last clone is dropped, the
        // writer thread observes EOF, signals Session::shutdown, and wakes the
        // blocking reader thread.
        Ok((self.tx.clone(), receiver))
    }
}

fn configure_adapter(name: &str, addresses: &[(IpAddr, IpAddr)], mtu: u16) -> io::Result<()> {
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TUN requires at least one interface address",
        ));
    }
    let luid = interface_luid(name)?;
    clear_manual_addresses(luid)?;
    for &(address, mask) in addresses {
        if address.is_ipv4() != mask.is_ipv4() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN address and mask families differ",
            ));
        }
        add_address(luid, address, crate::mask_to_prefix(mask)?)?;
        set_family_mtu(luid, address.is_ipv6(), mtu)?;
    }
    Ok(())
}

fn interface_luid(name: &str) -> io::Result<NET_LUID_LH> {
    let name = name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut luid = NET_LUID_LH::default();
    win32_result("resolve Wintun interface alias", unsafe {
        ConvertInterfaceAliasToLuid(name.as_ptr(), &mut luid)
    })?;
    Ok(luid)
}

fn clear_manual_addresses(luid: NET_LUID_LH) -> io::Result<()> {
    let mut table = std::ptr::null_mut();
    win32_result("enumerate Wintun addresses", unsafe {
        GetUnicastIpAddressTable(AF_UNSPEC, &mut table)
    })?;
    if table.is_null() {
        return Ok(());
    }
    let rows = unsafe {
        std::slice::from_raw_parts((*table).Table.as_ptr(), (*table).NumEntries as usize)
    };
    let luid_value = unsafe { luid.Value };
    let mut result = Ok(());
    for row in rows {
        if unsafe { row.InterfaceLuid.Value } != luid_value
            || row.PrefixOrigin != IpPrefixOriginManual
        {
            continue;
        }
        let status = unsafe { DeleteUnicastIpAddressEntry(row) };
        if status != 0 && status != ERROR_NOT_FOUND && result.is_ok() {
            result = Err(io::Error::from_raw_os_error(status as i32));
        }
    }
    unsafe { FreeMibTable(table.cast()) };
    result
}

fn add_address(luid: NET_LUID_LH, address: IpAddr, prefix: u8) -> io::Result<()> {
    let mut row = MIB_UNICASTIPADDRESS_ROW::default();
    unsafe { InitializeUnicastIpAddressEntry(&mut row) };
    row.InterfaceLuid = luid;
    row.Address = socket_address(address);
    row.OnLinkPrefixLength = prefix;
    let status = unsafe { CreateUnicastIpAddressEntry(&row) };
    if status == 0 || status == ERROR_OBJECT_ALREADY_EXISTS {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "configure Wintun address {address}/{prefix}: {}",
            io::Error::from_raw_os_error(status as i32)
        )))
    }
}

fn set_family_mtu(luid: NET_LUID_LH, ipv6: bool, mtu: u16) -> io::Result<()> {
    let mut row = MIB_IPINTERFACE_ROW::default();
    unsafe { InitializeIpInterfaceEntry(&mut row) };
    row.Family = if ipv6 { AF_INET6 } else { AF_INET };
    row.InterfaceLuid = luid;
    win32_result("read Wintun IP interface", unsafe {
        GetIpInterfaceEntry(&mut row)
    })?;
    // SetIpInterfaceEntry rejects a populated IPv4 SitePrefixLength even
    // when that field came directly from GetIpInterfaceEntry.
    if !ipv6 {
        row.SitePrefixLength = 0;
    }
    row.NlMtu = u32::from(mtu);
    win32_result("configure Wintun MTU", unsafe {
        SetIpInterfaceEntry(&mut row)
    })
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

fn win32_result(operation: &str, status: u32) -> io::Result<()> {
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{operation}: {}",
            io::Error::from_raw_os_error(status as i32)
        )))
    }
}

#[cfg(test)]
mod tests;
