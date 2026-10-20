use std::{
    io::{self, IoSliceMut},
    sync::Mutex,
    time::Instant,
};

use super::{log_sendmsg_error, RecvMeta, Transmit, UdpSockRef, IO_ERROR_LOG_INTERVAL};

/// Fallback UDP socket interface that stubs out all special functionality
///
/// Used when a better implementation is not available for a particular target, at the cost of
/// reduced performance compared to that enabled by some target-specific interfaces.
#[derive(Debug)]
pub struct UdpSocketState {
    last_send_error: Mutex<Instant>,
}

impl UdpSocketState {
    pub fn new(socket: UdpSockRef<'_>) -> io::Result<Self> {
        socket.0.set_nonblocking(true)?;
        let now = Instant::now();
        Ok(Self {
            last_send_error: Mutex::new(now.checked_sub(2 * IO_ERROR_LOG_INTERVAL).unwrap_or(now)),
        })
    }

    pub fn new_with_owned_probes(
        socket: UdpSockRef<'_>,
        _: &mut dyn crate::controlled::NativeProbes,
    ) -> io::Result<Self> {
        Self::new(socket)
    }
    pub fn try_send_outcome(
        &self,
        socket: UdpSockRef<'_>,
        transmit: &Transmit<'_>,
    ) -> io::Result<crate::controlled::SendOutcome> {
        if transmit.ecn.is_some() || transmit.src_ip.is_some() || transmit.segment_size.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native UDP metadata unavailable",
            ));
        }
        let bytes = socket.0.send_to(
            transmit.contents,
            &socket2::SockAddr::from(transmit.destination),
        )?;
        Ok(crate::controlled::SendOutcome::bytes(transmit, bytes))
    }

    /// Sends a [`Transmit`] on the given socket.
    ///
    /// This function will only ever return errors of kind [`io::ErrorKind::WouldBlock`].
    /// All other errors will be logged and converted to `Ok`.
    ///
    /// UDP transmission errors are considered non-fatal because higher-level protocols must
    /// employ retransmits and timeouts anyway in order to deal with UDP's unreliable nature.
    /// Thus, logging is most likely the only thing you can do with these errors.
    ///
    /// If you would like to handle these errors yourself, use [`UdpSocketState::try_send`]
    /// instead.
    pub fn send(&self, socket: UdpSockRef<'_>, transmit: &Transmit<'_>) -> io::Result<()> {
        match send(socket, transmit) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(e),
            Err(e) => {
                log_sendmsg_error(&self.last_send_error, e, transmit);

                Ok(())
            }
        }
    }

    /// Sends a [`Transmit`] on the given socket without any additional error handling.
    pub fn try_send(&self, socket: UdpSockRef<'_>, transmit: &Transmit<'_>) -> io::Result<()> {
        send(socket, transmit)
    }

    pub fn recv(
        &self,
        socket: UdpSockRef<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> io::Result<usize> {
        // Safety: both `IoSliceMut` and `MaybeUninitSlice` promise to have the
        // same layout, that of `iovec`/`WSABUF`. Furthermore `recv_vectored`
        // promises to not write unitialised bytes to the `bufs` and pass it
        // directly to the `recvmsg` system call, so this is safe.
        let bufs = unsafe {
            &mut *(bufs as *mut [IoSliceMut<'_>] as *mut [socket2::MaybeUninitSlice<'_>])
        };
        let (len, _flags, addr) = socket.0.recv_from_vectored(bufs)?;
        meta[0] = RecvMeta {
            len,
            stride: len,
            addr: addr.as_socket().unwrap(),
            ecn: None,
            dst_ip: None,
        };
        Ok(1)
    }

    #[inline]
    pub fn max_gso_segments(&self) -> usize {
        1
    }

    #[inline]
    pub fn gro_segments(&self) -> usize {
        1
    }

    /// Resize the send buffer of `socket` to `bytes`
    #[inline]
    pub fn set_send_buffer_size(&self, socket: UdpSockRef<'_>, bytes: usize) -> io::Result<()> {
        socket.0.set_send_buffer_size(bytes)
    }

    /// Resize the receive buffer of `socket` to `bytes`
    #[inline]
    pub fn set_recv_buffer_size(&self, socket: UdpSockRef<'_>, bytes: usize) -> io::Result<()> {
        socket.0.set_recv_buffer_size(bytes)
    }

    /// Get the size of the `socket` send buffer
    #[inline]
    pub fn send_buffer_size(&self, socket: UdpSockRef<'_>) -> io::Result<usize> {
        socket.0.send_buffer_size()
    }

    /// Get the size of the `socket` receive buffer
    #[inline]
    pub fn recv_buffer_size(&self, socket: UdpSockRef<'_>) -> io::Result<usize> {
        socket.0.recv_buffer_size()
    }

    #[inline]
    pub fn may_fragment(&self) -> bool {
        true
    }
}

fn send(socket: UdpSockRef<'_>, transmit: &Transmit<'_>) -> io::Result<()> {
    socket.0.send_to(
        transmit.contents,
        &socket2::SockAddr::from(transmit.destination),
    )
}

pub(crate) const BATCH_SIZE: usize = 1;

pub(crate) fn capability_without_probe(_: crate::controlled::ProbeKind) -> Option<usize> {
    Some(1)
}
pub(crate) fn probe_owned_socket(
    _: crate::controlled::ProbeKind,
    _: UdpSockRef<'_>,
) -> io::Result<usize> {
    Ok(1)
}
