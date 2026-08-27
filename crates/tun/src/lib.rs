//! Platform-agnostic TUN device abstraction.
//!
//! The `TunDevice` trait provides a unified async read/write interface
//! for virtual network interfaces across platforms.  Backends are selected
//! at compile time via `#[cfg(target_os)]`.

use std::io;
use std::net::IpAddr;

use tokio::io::{AsyncRead, AsyncWrite};

type TunPacketSender = tokio::sync::mpsc::Sender<Vec<u8>>;
type TunPacketReceiver = tokio::sync::mpsc::Receiver<Vec<u8>>;

mod route;
pub use route::{
    capture_route_prefixes, capture_route_prefixes_with_exclusions, split_default_route_prefixes,
    strict_route_socket_mark, RouteChangeMonitor, RouteInterface, SystemLeakGuard,
    SystemRouteGuard,
};

// ── Address helpers ───────────────────────────────────────────────────

/// Convert an IP + prefix to a netmask.
pub fn prefix_to_mask(prefix: u8, v6: bool) -> IpAddr {
    if v6 {
        let mask = u128::MAX
            .checked_shl(128u32.saturating_sub(prefix as u32))
            .unwrap_or(0);
        IpAddr::V6(std::net::Ipv6Addr::from(mask.to_be_bytes()))
    } else {
        let mask = u32::MAX
            .checked_shl(32u32.saturating_sub(prefix as u32))
            .unwrap_or(0);
        IpAddr::V4(std::net::Ipv4Addr::from(mask.to_be_bytes()))
    }
}

/// Convert a contiguous IPv4 or IPv6 netmask to its prefix length.
pub fn mask_to_prefix(mask: IpAddr) -> io::Result<u8> {
    let (normalized, prefix, width) = match mask {
        IpAddr::V4(mask) => {
            let bits = u32::from_be_bytes(mask.octets());
            ((bits as u128) << 96, bits.leading_ones() as u8, 32_u8)
        }
        IpAddr::V6(mask) => {
            let bits = u128::from_be_bytes(mask.octets());
            (bits, bits.leading_ones() as u8, 128_u8)
        }
    };
    let expected = if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix as u32)
    };
    if normalized != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("non-contiguous TUN netmask `{mask}`"),
        ));
    }
    Ok(prefix.min(width))
}

// ── Trait ─────────────────────────────────────────────────────────────

/// An async virtual network interface.
///
/// Reads produce raw IP packets (IPv4 or IPv6).  Writes send packets
/// back into the tunnel.
#[allow(async_fn_in_trait)]
pub trait TunDevice: AsyncRead + AsyncWrite + Send + Sync + Unpin {
    /// Bring the interface up with the given address, netmask, and MTU.
    fn configure(&self, addr: IpAddr, mask: IpAddr, mtu: u16) -> io::Result<()>;

    /// Bring the interface up with every requested address. Each address
    /// family is configured independently so one device can carry IPv4 and
    /// IPv6 traffic on strong-host platforms such as Windows.
    fn configure_addresses(&self, addresses: &[(IpAddr, IpAddr)], mtu: u16) -> io::Result<()> {
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TUN requires at least one interface address",
            ));
        }
        for &(address, mask) in addresses {
            self.configure(address, mask, mtu)?;
        }
        Ok(())
    }

    /// Return the interface name (e.g. "utun8", "tun0").
    fn name(&self) -> &str;

    /// Consume the device and return mpsc channel endpoints for
    /// reading and writing raw IP packets.
    ///
    /// Used when the OS owns the TUN lifecycle (iOS `NEPacketTunnelProvider`,
    /// Android `VpnService`) and the application only sees packet channels.
    /// Default implementation bridges `AsyncRead`/`AsyncWrite` via spawned tasks.
    fn into_channels(self) -> io::Result<(TunPacketSender, TunPacketReceiver)>
    where
        Self: Sized + 'static,
    {
        let (read_tx, read_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
        let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);

        let (mut reader, mut writer) = tokio::io::split(self);
        let (close_tx, mut close_rx) = tokio::sync::watch::channel(false);

        // Reader: TUN → channel
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                let n = tokio::select! {
                    read = tokio::io::AsyncReadExt::read(&mut reader, &mut buf) => read,
                    changed = close_rx.changed() => {
                        let _ = changed;
                        break;
                    }
                };
                match n {
                    Ok(0) => break,
                    Ok(n) => {
                        if read_tx.send(buf[..n].to_vec()).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        // Writer: channel → TUN
        tokio::spawn(async move {
            while let Some(pkt) = write_rx.recv().await {
                if tokio::io::AsyncWriteExt::write_all(&mut writer, &pkt)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let _ = close_tx.send(true);
        });

        Ok((write_tx, read_rx))
    }
}

// ── Platform backends ─────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::LinuxTun;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::Utun;

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub use windows::WindowsTun;

/// Create a new TUN device for the current platform.
///
/// Returns `None` if the platform is not yet supported.
pub fn create(name: Option<&str>) -> io::Result<impl TunDevice> {
    #[cfg(target_os = "linux")]
    {
        return linux::LinuxTun::create(name);
    }
    #[cfg(target_os = "macos")]
    {
        return macos::Utun::create(name);
    }
    #[cfg(target_os = "windows")]
    {
        return windows::WindowsTun::create(name);
    }
    #[allow(unreachable_code)]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "TUN is not yet supported on this platform",
        ))
    }
}
