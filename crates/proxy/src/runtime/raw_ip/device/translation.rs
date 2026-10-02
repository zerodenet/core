//! Packet adapter I/O; address/checksum/correlation semantics stay in zero-stack.

use std::{io, net::IpAddr};
use tokio::sync::mpsc;
use zero_stack::packet;

use super::{ForwardedPackets, SharedRawIpDevice};

impl SharedRawIpDevice {
    pub(crate) fn forward_translated_packet(
        &self,
        original: &[u8],
        local: IpAddr,
        replies: mpsc::Sender<Vec<u8>>,
        mtu: usize,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> io::Result<()> {
        if !self.is_usable() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "raw-IP device closed",
            ));
        }
        let permit = self
            .forwarded_packets
            .try_reserve()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let translated = self
            .returns
            .translate(original, local, replies, observer.clone())?;
        let packets = if translated.len() > mtu {
            // Only IPv4 packets without DF reach this branch; the route
            // operation has already returned the appropriate ICMP MTU error.
            let packets = packet::fragment_forwarded_packet(&translated, mtu);
            if packets.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid translated IP fragment",
                ));
            }
            packets
        } else {
            vec![translated]
        };
        permit.send(ForwardedPackets { packets, observer });
        Ok(())
    }
}
