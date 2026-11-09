//! Existing finite FIFO, with actual table memory paid for hosted Sources.
use super::*;
use std::collections::VecDeque;
pub(super) enum Queue {
    Library(VecDeque<UdpStackDatagram>),
    #[cfg(feature = "packet-memory")]
    Owned(zero_tun::packet_memory::PacketMetadataVec<UdpStackDatagram>),
}
impl Queue {
    pub(super) fn new(outbound: &crate::packet_output::PacketSender) -> Self {
        #[cfg(feature = "packet-memory")]
        if outbound.has_received_memory() {
            return Self::Owned(Default::default());
        }
        #[cfg(not(feature = "packet-memory"))]
        let _ = outbound;
        Self::Library(VecDeque::new())
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Library(rows) => rows.len(),
            #[cfg(feature = "packet-memory")]
            Self::Owned(rows) => rows.len(),
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn push(
        &mut self,
        datagram: UdpStackDatagram,
        #[cfg(feature = "packet-memory")] memory: Option<
            &zero_tun::packet_memory::PacketMemoryView,
        >,
    ) -> Result<(), ()> {
        match self {
            Self::Library(rows) => {
                rows.push_back(datagram);
                Ok(())
            }
            #[cfg(feature = "packet-memory")]
            Self::Owned(rows) => {
                let Some(memory) = memory else {
                    return Err(());
                };
                rows.try_push(datagram, memory, 1024).map_err(|_| ())
            }
        }
    }
    // Return the old empty table for actual deallocation/notification unlocked.
    pub(super) fn pop(&mut self) -> (Option<UdpStackDatagram>, Option<Self>) {
        match self {
            Self::Library(rows) => (rows.pop_front(), None),
            #[cfg(feature = "packet-memory")]
            Self::Owned(rows) => {
                if rows.is_empty() {
                    return (None, None);
                }
                let datagram = rows.remove(0);
                let empty = rows.is_empty().then(|| Self::Owned(std::mem::take(rows)));
                (Some(datagram), empty)
            }
        }
    }
}
