//! Only the actual next Ready packet allocates a Domain, with its original ticket.
use super::*;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout};
pub(super) struct Held {
    original: Option<TunDatagram>,
    ready: Option<SniffedTunDatagram>,
    pub(super) name: Option<crate::runtime::sniff::inline::InlineName>,
    credit: Option<PacketMetadataCredit>,
}
impl Held {
    pub(super) fn new(
        original: TunDatagram,
        name: Option<crate::runtime::sniff::inline::InlineName>,
    ) -> Self {
        Self {
            original: Some(original),
            ready: None,
            name,
            credit: None,
        }
    }
    pub(super) fn prepare(
        &mut self,
        memory: Option<&PacketDriverMetadataView>,
    ) -> Result<(), PacketMemoryError> {
        if self.ready.is_some() {
            return Ok(());
        }
        let (target, host_source) = match self.name {
            Some(name) => {
                if let Some(memory) = memory {
                    if self.credit.is_none() {
                        self.credit = Some(
                            memory
                                .reserve(PacketMetadataLayout::array::<u8>(name.as_str().len())?)?,
                        );
                    }
                }
                let bytes = Box::<[u8]>::from(name.as_str().as_bytes()).into_vec();
                (
                    Address::Domain(String::from_utf8(bytes).expect("same ASCII name")),
                    Some(TargetHostSource::QuicSni),
                )
            }
            None => {
                let destination = self.original.as_ref().expect("same held input").destination;
                (
                    match destination.ip {
                        zero_traits::IpAddress::V4(ip) => Address::Ipv4(ip),
                        zero_traits::IpAddress::V6(ip) => Address::Ipv6(ip),
                    },
                    None,
                )
            }
        };
        let original = self.original.take().expect("same held original");
        self.ready = Some(SniffedTunDatagram {
            peer_identity: original.peer_identity,
            original_destination: original.destination,
            target,
            payload: original.payload,
            host_source,
            domain_metadata: self.credit.take(),
        });
        Ok(())
    }
    pub(super) fn into_ready(mut self) -> SniffedTunDatagram {
        self.ready
            .take()
            .expect("same next packet already paid before delivery")
    }
}
