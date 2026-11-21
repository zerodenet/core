//! Typed access to this same packet owner's raw stream for existing upper pools.
//! Weak identities and lightweight lifecycle fences create no second pool, task
//! or native owner. Original receipts and Host Scope joins stay authoritative.
use super::*;
mod binding;
mod options;
use std::collections::HashMap;
use zero_core::{
    component::ComponentInstanceId,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::{
    contract::{
        context::{OpenContext, RequestIdentity, ServiceFactsOwner},
        network::{CarrierContext, PendingCarrierObserver, UpstreamBinding},
    },
    controlled_carrier::loan::StreamLoan,
};
use zero_traits::packet_metadata::PacketMetadataWeak;
type Slots = HashMap<Vec<u8>, Arc<dyn SlotAccess>>;
#[derive(Clone, Default)]
pub(crate) struct Registry(Arc<Mutex<Slots>>);
struct Registered {
    owner: binding::WeakOwner,
    closed: PacketMetadataArc<std::sync::atomic::AtomicBool>,
}
struct Slot(Mutex<Option<Registered>>);
// The same cold slot exposes only its typed binding operations. Its view must
// not expand Options -> slot -> installed owner -> Options in every consumer.
trait SlotAccess: Send + Sync {
    fn install(&self, generation: binding::Owner) -> Result<(), ChannelError>;
    fn owner(&self) -> Option<binding::WeakOwner>;
    fn reclaimable(&self) -> bool;
}
#[derive(Clone)]
pub struct TransportCarrier {
    slot: Arc<dyn SlotAccess>,
    endpoint: Destination,
}
impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VlessPacketBindings")
    }
}
impl std::fmt::Debug for TransportCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessTransportCarrier")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl Registry {
    pub(in crate::component) fn access(
        &self,
        identity: &[u8],
        endpoint: Destination,
    ) -> TransportCarrier {
        let mut slots = self.0.lock().unwrap();
        let reclaimable = slots
            .iter()
            .filter(|(_, slot)| Arc::strong_count(slot) == 1 && slot.reclaimable())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let retired = reclaimable
            .into_iter()
            .filter_map(|key| slots.remove(&key))
            .collect::<Vec<_>>();
        let slot = slots.get(identity).cloned().unwrap_or_else(|| {
            let slot: Arc<dyn SlotAccess> = Arc::new(Slot(Mutex::new(None)));
            slots.insert(identity.to_vec(), slot.clone());
            slot
        });
        drop(slots);
        drop(retired);
        TransportCarrier { slot, endpoint }
    }
}
impl SlotAccess for Slot {
    fn install(&self, generation: binding::Owner) -> Result<(), ChannelError> {
        let mut slot = self.0.lock().unwrap();
        if let Some(previous) = &*slot {
            if previous.owner.same_target(&generation.downgrade()) {
                return Ok(());
            }
            if !previous.closed.load(std::sync::atomic::Ordering::Acquire) {
                return Err(error(Stage::Prepare, Reason::StalePlan));
            }
        }
        let previous = slot.replace(Registered {
            owner: generation.downgrade(),
            closed: generation.closed_marker(),
        });
        drop(slot);
        drop(previous);
        Ok(())
    }

    fn owner(&self) -> Option<binding::WeakOwner> {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .map(|record| record.owner.clone())
    }

    fn reclaimable(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|record| record.closed.load(std::sync::atomic::Ordering::Acquire))
    }
}
impl TransportCarrier {
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub(super) fn install(
        &self,
        generation: &PacketMetadataArc<entry::Generation>,
    ) -> Result<(), ChannelError> {
        self.install_owner(binding::Owner::Packet(generation.clone()))
    }
    fn install_owner(&self, generation: binding::Owner) -> Result<(), ChannelError> {
        self.slot.install(generation)
    }
    fn generation(&self, expected: ComponentInstanceId) -> Result<binding::Owner, ChannelError> {
        self.slot
            .owner()
            .and_then(|owner| owner.upgrade())
            .filter(|g| g.instance() == expected && !g.cancelled() && !g.closed())
            .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))
    }
    pub(in crate::component) fn matches(
        &self,
        expected: ComponentInstanceId,
    ) -> Result<(), ChannelError> {
        self.generation(expected).map(|_| ())
    }
    pub async fn open_carrier(
        &self,
        expected: ComponentInstanceId,
        binding: UpstreamBinding,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<StreamLoan, ChannelError> {
        if call.session.is_some() || call.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::InvalidConfig));
        }
        let generation = self.generation(expected)?;
        let upstream = match waiting {
            Some(waiting) => {
                generation
                    .upstream()
                    .select_background_carrier_observed(Some(binding), call.clone(), waiting)
                    .await?
            }
            None => {
                generation
                    .upstream()
                    .select_background_carrier(Some(binding), call.clone())
                    .await?
            }
        };
        let credit = generation.memory(
            ServiceFactsOwner::paid_layout()
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?,
        )?;
        let identity = ServiceFactsOwner::from_prepaid(
            ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            },
            credit,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        let context = OpenContext {
            identity: RequestIdentity::Service(identity),
            deadline: call.deadline,
            cancel: call.cancel,
            upstream,
            observer: None,
        };
        let opened = generation.open(&context).await?;
        let bypass = zero_traits::AsyncSocket::transport_bypass_control(&opened.io);
        Ok(
            StreamLoan::new(Box::pin(opened.io), opened.control, opened.original)
                .with_transport_bypass_control(bypass),
        )
    }
    pub(in crate::component) async fn open(
        &self,
        expected: ComponentInstanceId,
        context: &OpenContext,
    ) -> Result<crate::component::preconnect::Opened, ChannelError> {
        self.generation(expected)?.open(context).await
    }
    async fn open_original(
        generation: &PacketMetadataArc<entry::Generation>,
        context: &OpenContext,
    ) -> Result<crate::component::preconnect::Opened, ChannelError> {
        if generation.options.hysteria.is_some() {
            return super::hysteria::open(generation, context).await;
        }
        let initialized = entry::Generation::acquire(generation, context).await?;
        let entry = initialized.entry.clone();
        let original = entry
            .origin
            .get()
            .cloned()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let io = initialized.into_relay();
        let memory = entry
            .owners
            .state
            .lock()
            .await
            .logical_memory
            .take()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let control = zero_platform_tokio::contract::lifecycle::BoxChannelControl::from_prepaid(
            entry::LogicalControl(entry),
            memory,
        )
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
        Ok(crate::component::preconnect::Opened {
            io,
            control,
            original,
        })
    }
}

#[cfg(test)]
#[path = "../../../tests/component/quic_carrier.rs"]
mod tests;

/// Additional weak-handle reuse fence. It is never a native/user/Scope close
/// receipt: the actual Host transaction must still confirm every original.
pub(in crate::component) fn closed_marker(
    memory: &zero_platform_tokio::contract::component::PacketDriverMetadataView,
) -> Result<PacketMetadataArc<std::sync::atomic::AtomicBool>, ChannelError> {
    let layout = PacketMetadataArc::<std::sync::atomic::AtomicBool>::layout()
        .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    let credit = memory
        .reserve(layout)
        .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source))?;
    PacketMetadataArc::from_prepaid(false.into(), credit)
        .map_err(|(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source))
}
