//! Best-effort source-port preservation on the same original UDP socket.
use std::{io, net::SocketAddr};

pub(crate) fn bind_source(
    socket: &socket2::SockRef<'_>,
    mut source: SocketAddr,
    preferred_port: Option<u16>,
) -> io::Result<SocketAddr> {
    #[cfg(windows)]
    if preferred_port.is_some_and(|port| port != 0) {
        crate::windows_socket::set_exclusive_addr_use(socket, true)?;
    }
    match socket.bind(&source.into()) {
        Ok(()) => {}
        Err(error) if can_retry_port(preferred_port, &error) => {
            #[cfg(windows)]
            crate::windows_socket::set_exclusive_addr_use(socket, false)?;
            // A rejected bind leaves this original unbound. Change only the
            // preferred port, once; keep its address, family and socket options.
            source.set_port(0);
            socket.bind(&source.into())?;
        }
        Err(error) => return Err(error),
    }
    Ok(source)
}

fn can_retry_port(preferred_port: Option<u16>, error: &io::Error) -> bool {
    if !preferred_port.is_some_and(|port| port != 0) {
        // Exact listeners and already-ephemeral binds must never be relaxed.
        return false;
    }
    if error.kind() == io::ErrorKind::AddrInUse {
        return true;
    }
    #[cfg(windows)]
    if windows_port_conflict(error) {
        return true;
    }
    false
}

#[cfg(any(windows, test))]
fn windows_port_conflict(error: &io::Error) -> bool {
    // Windows reports an exclusive/reserved port as WSAEACCES, not necessarily
    // WSAEADDRINUSE. Do not treat generic permission failures on Unix (or a
    // contextual error without this original native code) as port conflicts.
    error.raw_os_error() == Some(10013)
}

#[cfg(test)]
#[path = "../../tests/dial_policy/udp_bind.rs"]
mod tests;
