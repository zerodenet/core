use super::*;
#[derive(Clone)]
pub struct CarrierOriginClosed(pub(super) ClosedOriginal);
#[derive(Clone)]
pub(super) enum ClosedOriginal {
    Carrier(Arc<std::sync::OnceLock<bool>>),
    Accepted(crate::controlled_listener::accepted::AcceptedTcpClosed),
    Packet(PacketProviderClosed),
    Listener(crate::controlled_datagram::listener::origin::ListenerProviderClosed),
    Metadata(crate::controlled_datagram::metadata_pool::MetadataPoolClosed),
    MetadataPool(crate::controlled_datagram::metadata_pool::MetadataPoolOwnerClosed),
}
impl CarrierOriginClosed {
    pub(in crate::controlled_carrier) fn carrier(
        completion: Arc<std::sync::OnceLock<bool>>,
    ) -> Self {
        Self(ClosedOriginal::Carrier(completion))
    }
    pub(in crate::controlled_carrier) fn carrier_completion(
        &self,
    ) -> Option<&Arc<std::sync::OnceLock<bool>>> {
        match &self.0 {
            ClosedOriginal::Carrier(completion) => Some(completion),
            _ => None,
        }
    }
    pub fn matches(&self, origin: &CarrierOrigin) -> bool {
        match (&self.0, origin.observation()) {
            (ClosedOriginal::Accepted(closed), OriginalRef::Accepted(origin)) => {
                origin.matches_closed(closed)
            }
            (ClosedOriginal::Packet(closed), OriginalRef::Packet(origin)) => closed.matches(origin),
            (ClosedOriginal::Listener(closed), OriginalRef::Listener(origin)) => {
                closed.matches(origin.owner())
            }
            (ClosedOriginal::Carrier(completion), OriginalRef::Carrier(origin)) => {
                Arc::ptr_eq(completion, &origin.completion) && completion.get().is_some()
            }
            (ClosedOriginal::Metadata(closed), OriginalRef::Metadata(origin)) => {
                closed.matches(origin)
            }
            (
                ClosedOriginal::MetadataPool(closed),
                OriginalRef::MetadataPool {
                    owner,
                    binding: Some(_),
                },
            ) => closed.matches(owner),
            _ => false,
        }
    }
    pub fn had_native_acquisition(&self) -> bool {
        match &self.0 {
            ClosedOriginal::Accepted(_)
            | ClosedOriginal::Packet(_)
            | ClosedOriginal::Listener(_) => true,
            ClosedOriginal::Carrier(completion) => completion.get() == Some(&true),
            ClosedOriginal::Metadata(closed) => closed.had_native_acquisition(),
            ClosedOriginal::MetadataPool(closed) => closed.had_native_acquisition(),
        }
    }
}
