//! A host-owned Wintun adapter: no creation, addressing or route changes.
use super::{
    dad_state_is_ready, interface_luid, load_wintun, require_elevated_process, socket_address,
    win32_result,
};
use std::{
    future::Future,
    io,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    thread::JoinHandle,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
    time::Sleep,
};
use windows_sys::Win32::{
    Foundation::ERROR_BUFFER_OVERFLOW,
    NetworkManagement::IpHelper::{
        GetIpInterfaceEntry, GetUnicastIpAddressEntry, InitializeIpInterfaceEntry,
        InitializeUnicastIpAddressEntry, MIB_IPINTERFACE_ROW, MIB_UNICASTIPADDRESS_ROW,
    },
    Networking::WinSock::{AF_INET, AF_INET6},
};

/// Open an existing dedicated adapter. The host retains its adapter handle
/// and supplies all network configuration and exclusive packet-I/O ownership.
pub struct ExistingWindowsTun {
    name: String,
    session: Arc<wintun::Session>,
    rx: Option<mpsc::Receiver<io::Result<Vec<u8>>>>,
    reader: Option<JoinHandle<()>>,
    write_retry: Option<Pin<Box<Sleep>>>,
}

impl ExistingWindowsTun {
    pub fn open(name: &str, addresses: &[IpAddr], mtu: u16) -> io::Result<Self> {
        require_elevated_process()?;
        if name.is_empty()
            || name.contains('\0')
            || name.encode_utf16().count() > 128
            || addresses.is_empty()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "existing Wintun requires an interface and router addresses",
            ));
        }
        let driver = load_wintun()?;
        // Deliberately no create fallback: a missing host device is an error.
        let adapter = wintun::Adapter::open(&driver, name).map_err(|error| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("open existing host Wintun adapter: {error}"),
            )
        })?;
        validate_addresses(name, addresses, mtu)?;
        let session = Arc::new(adapter.start_session(wintun::MAX_RING_CAPACITY).map_err(
            |error| io::Error::other(format!("start existing host Wintun session: {error}")),
        )?);
        Ok(Self {
            name: name.into(),
            session,
            rx: None,
            reader: None,
            write_retry: None,
        })
    }

    fn start_reader(&mut self) -> io::Result<()> {
        if self.rx.is_some() {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel(256);
        let session = self.session.clone();
        let reader = std::thread::Builder::new()
            .name("zero-host-wintun".into())
            .spawn(move || loop {
                let data = session
                    .receive_blocking()
                    .map(|packet| packet.bytes().to_vec())
                    .map_err(io::Error::from);
                let failed = data.is_err();
                if tx.blocking_send(data).is_err() || failed {
                    break;
                }
            })?;
        self.rx = Some(rx);
        self.reader = Some(reader);
        Ok(())
    }
}

fn validate_addresses(name: &str, addresses: &[IpAddr], mtu: u16) -> io::Result<()> {
    let luid = interface_luid(name)?;
    for &address in addresses {
        let mut row = MIB_UNICASTIPADDRESS_ROW::default();
        unsafe { InitializeUnicastIpAddressEntry(&mut row) };
        row.InterfaceLuid = luid;
        row.Address = socket_address(address);
        win32_result("query host Wintun router address", unsafe {
            GetUnicastIpAddressEntry(&mut row)
        })?;
        if !dad_state_is_ready(address, row.DadState)? {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "host Wintun router address is not ready",
            ));
        }
        let mut family = MIB_IPINTERFACE_ROW::default();
        unsafe { InitializeIpInterfaceEntry(&mut family) };
        family.InterfaceLuid = luid;
        family.Family = if address.is_ipv4() { AF_INET } else { AF_INET6 };
        win32_result("query host Wintun MTU", unsafe {
            GetIpInterfaceEntry(&mut family)
        })?;
        if u32::from(mtu) > family.NlMtu {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet MTU exceeds host Wintun MTU",
            ));
        }
    }
    Ok(())
}

impl AsyncRead for ExistingWindowsTun {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.start_reader()?;
        match self.rx.as_mut().expect("reader initialized").poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) if data.len() <= buf.remaining() => {
                buf.put_slice(&data);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host packet receive buffer too small",
            ))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for ExistingWindowsTun {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Some(retry) = &mut self.write_retry {
            if retry.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            self.write_retry = None;
        }
        let len = u16::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "oversized host IP packet"))?;
        match self.session.allocate_send_packet(len) {
            Ok(mut packet) => {
                packet.bytes_mut().copy_from_slice(buf);
                self.session.send_packet(packet);
                // Completion is driver acceptance, never just a queued write.
                Poll::Ready(Ok(buf.len()))
            }
            Err(error) if buffer_full(&error) => {
                let mut retry = Box::pin(tokio::time::sleep(Duration::from_millis(1)));
                let _ = retry.as_mut().poll(cx);
                self.write_retry = Some(retry);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(error.into())),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn buffer_full(error: &wintun::Error) -> bool {
    match error {
        wintun::Error::Io(error) => error.raw_os_error() == Some(ERROR_BUFFER_OVERFLOW as i32),
        wintun::Error::WindowsCore(error) => {
            error.code().0 as u32 == (0x80070000 | ERROR_BUFFER_OVERFLOW)
        }
        _ => false,
    }
}

impl crate::TunDevice for ExistingWindowsTun {
    fn name(&self) -> &str {
        &self.name
    }
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "host-owned Wintun configuration is read-only",
        ))
    }
}

impl Drop for ExistingWindowsTun {
    fn drop(&mut self) {
        // Close the bounded queue first, including a reader blocked on delivery.
        drop(self.rx.take());
        let _ = self.session.shutdown();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

#[cfg(test)]
#[path = "existing/tests.rs"]
mod tests;
