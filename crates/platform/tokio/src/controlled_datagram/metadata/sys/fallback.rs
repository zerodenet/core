use std::io::{self, IoSliceMut};

use socket2::SockRef;

use super::{NativeProbes, ProbeKind};
use crate::controlled_datagram::metadata::{
    MetadataReceive, MetadataSendOutcome, MetadataTransmit,
};

pub(in super::super) const BATCH_SIZE: usize = 1;

#[derive(Debug)]
pub(in super::super) struct UdpSocketState;

impl UdpSocketState {
    pub(in super::super) fn new_with_owned_probes(
        socket: SockRef<'_>,
        _: &mut dyn NativeProbes,
    ) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(Self)
    }

    pub(in super::super) fn try_send_outcome(
        &self,
        socket: SockRef<'_>,
        transmit: &MetadataTransmit<'_>,
    ) -> io::Result<MetadataSendOutcome> {
        if transmit.ecn.is_some() || transmit.source_ip.is_some() || transmit.segment_size.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native UDP metadata unavailable",
            ));
        }
        let bytes = socket.send_to(
            transmit.contents,
            &socket2::SockAddr::from(transmit.destination),
        )?;
        Ok(MetadataSendOutcome::from_bytes(transmit, bytes))
    }

    pub(in super::super) fn recv(
        &self,
        socket: SockRef<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [MetadataReceive],
    ) -> io::Result<usize> {
        let bufs = unsafe {
            &mut *(bufs as *mut [IoSliceMut<'_>] as *mut [socket2::MaybeUninitSlice<'_>])
        };
        let (len, _flags, addr) = socket.recv_from_vectored(bufs)?;
        meta[0] = MetadataReceive {
            source: addr.as_socket().unwrap(),
            destination_ip: None,
            len,
            stride: len,
            ecn: None,
        };
        Ok(1)
    }

    #[inline]
    pub(in super::super) fn max_gso_segments(&self) -> usize {
        1
    }

    #[inline]
    pub(in super::super) fn gro_segments(&self) -> usize {
        1
    }

    #[inline]
    pub(in super::super) fn may_fragment(&self) -> bool {
        true
    }
}

pub(super) fn capability_without_probe(_: ProbeKind) -> Option<usize> {
    Some(1)
}

pub(super) fn probe_owned_socket(_: ProbeKind, _: SockRef<'_>) -> io::Result<usize> {
    Ok(1)
}
